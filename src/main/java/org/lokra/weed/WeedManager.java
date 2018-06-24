package org.lokra.weed;

import feign.Feign;
import feign.form.FormEncoder;
import feign.jackson.JacksonDecoder;
import feign.okhttp.OkHttpClient;
import org.lokra.weed.content.Cluster;
import org.lokra.weed.content.Locations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.stream.Collectors;

public class WeedManager {
  private final Logger log = LoggerFactory.getLogger(WeedManager.class);

  private ConcurrentMap<String, WeedVolumeClient> volumeClients = new ConcurrentHashMap<>();

  private ConcurrentMap<String, Set<String>> volumeCollectionKeys = new ConcurrentHashMap<>();

  // other known masters - besides of the one used on init
  private ConcurrentMap<String, WeedMasterClient> peerMasters = new ConcurrentHashMap<>();

  // background health checks
  private final MasterHealthCheckThread masterHealthCheckThread = new MasterHealthCheckThread();
  private final VolumeHealthCheckThread volumeHealthCheckThread =
      new VolumeHealthCheckThread(); // not yet implemented

  private String leaderMasterUrl;
  private WeedMasterClient leaderMaster;
  private String host;
  private int port;
  private ConcurrentSkipListSet<WeedVolumeClient> unavailableClients =
      new ConcurrentSkipListSet<>();

  public WeedManager() {}

  /**
   * Construction.
   *
   * @param host Master server host.
   * @param port Master server port.
   */
  public WeedManager(String host, int port) {
    this.host = host;
    this.port = port;
  }

  /** Startup weed manager. */
  public void start() {
    leaderMasterUrl = assembleUrl(host, port);
    leaderMaster =
        Feign.builder()
            .client(new OkHttpClient())
            .decoder(new JacksonDecoder())
            .target(WeedMasterClient.class, String.format("http://%s", leaderMasterUrl));

    fetchMasters(leaderMaster.cluster());

    masterHealthCheckThread.start();
    volumeHealthCheckThread.start();
  }

  /** Shutdown weed manager. */
  public void shutdown() {
    masterHealthCheckThread.interrupt();
    volumeHealthCheckThread.interrupt();
  }

  /**
   * Get master server client.
   *
   * @return client.
   */
  public WeedMasterClient getMasterClient() {
    return this.leaderMaster;
  }

  /**
   * Get volume server client for a given Volume Id an collection. We use one separate instantiated
   * client per volume server.
   *
   * @param volumeId
   * @param collection
   * @return client.
   */
  public WeedVolumeClient getVolumeClient(int volumeId, String collection) {

    String collectionKey = String.format("%d#%s", volumeId, collection != null ? collection : "");

    // Volume urls already registered?
    Set<String> volumeClientKeys = volumeCollectionKeys.get(collectionKey);
    if (volumeClientKeys == null || volumeClientKeys.size() == 0) {
      volumeClientKeys = fetchVolumeUrls(volumeId, collection);
      volumeCollectionKeys.put(
          collectionKey,
          volumeClientKeys); // why don't we use a set? blank key does not make sense?
    }

    // poor man's load balancing? we do not handle volumes not available here?
    return selectVolumeClient(volumeClientKeys);
  }

  private WeedVolumeClient selectVolumeClient(Set<String> volumeClientKeys) {
    int clientCountForVolume = volumeClientKeys.size();
    final long millis = System.currentTimeMillis();
    final int selectedIdx = (int) (millis % clientCountForVolume);

    final String[] vcKeys = new String[volumeClientKeys.size()];
    volumeClientKeys.toArray(vcKeys);

    WeedVolumeClient selectedClient = volumeClients.get(vcKeys[selectedIdx]);

    if (unavailableClients.contains(selectedClient)) {
      // just loop
      for (int i = 0; i < clientCountForVolume; i++) {
        if (selectedIdx == i) continue;

        selectedClient = volumeClients.get(vcKeys[i]);
        if (unavailableClients.contains(selectedClient)) continue;
        return selectedClient;
      }
      throw new IllegalStateException("No volume client available");
    }

    return selectedClient;
  }

  /**
   * Fetch volume server clients for a given volume id and (optional) collection.
   *
   * @param volumeId Volume id, e.g. 01, 02, nn.
   * @param collection Collection.
   * @return client keys for the volume, for lookup of corresponding client.
   */
  private Set<String> fetchVolumeUrls(int volumeId, String collection) {
    // in case of replication we may have multiple locations for a volume
    Locations locations = getMasterClient().lookup(volumeId, collection);

    Set<String> ids = new HashSet<>();
    locations
        .getLocations()
        .forEach(
            location -> {
              // TODO what is the idea behind this 'id' made up from two (equal!) URLs?
              final String id = String.format("%s#%s", location.getUrl(), location.getUrl());
              ids.add(id);
              if (!volumeClients.containsKey(id)) {
                volumeClients.put(
                    id,
                    Feign.builder()
                        .client(new OkHttpClient())
                        .encoder(new FormEncoder())
                        .decoder(new JacksonDecoder())
                        .target(
                            WeedVolumeClient.class,
                            String.format("http://%s", location.getPublicUrl())));
              }
            });
    return ids;
  }

  /**
   * Master server host.
   *
   * @return host.
   */
  public String getHost() {
    return host;
  }

  /**
   * Master server port.
   *
   * @return port.
   */
  public int getPort() {
    return port;
  }

  /**
   * Assemble URL.
   *
   * @param host Host.
   * @param port Port.
   * @return URL.
   */
  private String assembleUrl(String host, int port) {
    return String.format("%s:%d", host, port);
  }

  /**
   * Fetch master cluster information.
   *
   * @param cluster Master cluster information.
   */
  private void fetchMasters(Cluster cluster) {
    if (!leaderMasterUrl.equals(cluster.getLeader())) {
      leaderMasterUrl = cluster.getLeader();
      log.info("Weed leader master server is change to [{}]", leaderMasterUrl);
      leaderMaster =
          Feign.builder()
              .client(new OkHttpClient())
              .decoder(new JacksonDecoder())
              .target(WeedMasterClient.class, String.format("http://%s", leaderMaster));
    }

    // Cleanup peer master
    Set<String> removeSet =
        peerMasters
            .keySet()
            .stream()
            .filter(key -> !cluster.getPeers().contains(key) && !cluster.getLeader().equals(key))
            .collect(Collectors.toSet());

    peerMasters.remove(leaderMasterUrl);
    removeSet.forEach(key -> peerMasters.remove(key));
    if (null != cluster.getPeers()) {
      cluster
          .getPeers()
          .forEach(
              url -> {
                if (!peerMasters.containsKey(url)) {
                  peerMasters.put(
                      url,
                      Feign.builder()
                          .client(new OkHttpClient())
                          .decoder(new JacksonDecoder())
                          .target(WeedMasterClient.class, String.format("http://%s", url)));
                }
              });
    }
  }

  /** Master server health check thread */
  class MasterHealthCheckThread extends Thread {

    @Override
    public void run() {
      log.info("Started weed leader master server health check");
      //noinspection InfiniteLoopStatement
      while (true) {
        try {
          Thread.sleep(3000);
        } catch (InterruptedException e) {
          log.info("Stop weed leader master server health check");
          return;
        }
        try {
          fetchMasters(leaderMaster.cluster());
        } catch (Exception e) {
          log.warn("Weed leader master server [{}] is down", leaderMasterUrl);
          // renew list of remaining masters / re-elected
          for (String url : peerMasters.keySet()) {
            try {
              fetchMasters(peerMasters.get(url).cluster());
            } catch (Exception ignored) {
              log.warn("Weed peer master server [{}] is down", url);
              continue;
            }
            break;
          }
        }
      }
    }
  }

  /** Volume server health check thread */
  class VolumeHealthCheckThread extends Thread {
    @Override
    public void run() {
      log.info("Started weed volume server health check");
      //noinspection InfiniteLoopStatement
      while (true) {
        try {
          Thread.sleep(3000);
        } catch (InterruptedException e) {
          log.info("Stop weed leader master server health check");
          return;
        }
        for (WeedVolumeClient client : volumeClients.values()) {
          try {
            client.checkAlive();
            unavailableClients.remove(client);
          } catch (Exception e) {
            unavailableClients.add(client);
          }
        }
      }
    }
  }
}
