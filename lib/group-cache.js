const NodeCache = require('node-cache');

class GroupCache {
  constructor(ttl = 300) {
    this.cache = new NodeCache({
      stdTTL: ttl,
      useClones: false,
      checkperiod: 600
    });
  }
  get(jid) {
    return this.cache.get(jid);
  }
  set(jid, metadata) {
    return this.cache.set(jid, metadata);
  }
  delete(jid) {
    return this.cache.del(jid);
  }
  clear() {
    return this.cache.flushAll();
  }
  getStats() {
    return this.cache.getStats();
  }
  has(jid) {
    return this.cache.has(jid);
  }

  async refresh(sock, jid) {
    try {
      const metadata = await sock.groupMetadata(jid);
      this.set(jid, metadata);
      return metadata;
    } catch (error) {
      console.error(`Error refreshing group ${jid}:`, error.message);
      throw error;
    }
  }
}

const groupCache = new GroupCache(300);

module.exports = groupCache;