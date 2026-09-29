/* API-key storage and the rule for which 401s may clear it. A classic script
 * loaded before the app script in index.html; kept out of the inline block so
 * web/test/auth-client.test.js can run it. */
(function(root) {
  'use strict';

  /* Credentials are kept client-side on a same-origin page. Admin routes take a
   * second key so a normal operator cannot resize the billable node pool — and
   * because that key can spend money, it lives in sessionStorage and dies with
   * the tab, rather than sitting in localStorage on a shared machine. */
  var Auth = {
    KEY_ITEM: 'api.key',
    ADMIN_ITEM: 'api.adminKey',
    onUnauthorized: null,
    /* Whether this deployment demands a key. Assumed true until /api/health
     * says otherwise, so a slow probe never leaks requests unauthenticated.
     * The desktop build reports false and has no key at all. */
    required: true,
    store: function(item) {
      return item === Auth.ADMIN_ITEM ? sessionStorage : localStorage;
    },
    get: function(item) {
      try { return Auth.store(item).getItem(item) || ''; } catch (e) { return ''; }
    },
    set: function(item, value) {
      try { Auth.store(item).setItem(item, value || ''); } catch (e) {}
    },
    key: function() { return Auth.get(Auth.KEY_ITEM); },
    adminKey: function() { return Auth.get(Auth.ADMIN_ITEM); },
    clearKey: function() { Auth.set(Auth.KEY_ITEM, ''); },
    needsAdmin: function(url, method) {
      if (url.indexOf('/api/cluster/workers') === 0) return true;
      return url === '/api/jobs' && String(method || 'GET').toUpperCase() === 'DELETE';
    },
    headers: function(url, opts) {
      var h = Object.assign({}, (opts && opts.headers) || {});
      var k = Auth.key();
      if (k) h['X-API-Key'] = k;
      if (Auth.needsAdmin(url, opts && opts.method)) {
        var a = Auth.adminKey();
        if (a) h['X-Admin-Key'] = a;
      }
      return h;
    },
    /* A 401 (or WebSocket 4401) answers the key that request carried, not
     * whatever is stored by the time it lands. Callers pass the key they sent.
     * A rejection of a key the user has already replaced, or of a keyless
     * request that raced a newly entered key, must leave the stored key and the
     * input alone; otherwise every poll in flight at entry wipes a valid key. */
    rejected: function(sentKey) {
      var current = Auth.key();
      if (sentKey) {
        if (sentKey !== current) return;
        Auth.clearKey();
        if (Auth.onUnauthorized) Auth.onUnauthorized(true);
        return;
      }
      if (current) return;
      if (Auth.onUnauthorized) Auth.onUnauthorized(false);
    }
  };

  root.Auth = Auth;
})(window);
