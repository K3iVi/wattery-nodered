'use strict';
/** POST JSON na server wattery (modul `https` / `http` — v uzlu funkce Node-RED na Venus `fetch` není). */
const https = require('https');
const http = require('http');

const TIMEOUT_MS = 20000;
const MAX_RESPONSE = 1024 * 1024;

function makePost(baseUrl, { userAgent = 'wattery-nodered' } = {}) {
  const base = new URL(baseUrl);
  const mod = base.protocol === 'http:' ? http : https;
  return (path, body, token) => new Promise((resolve, reject) => {
    const data = Buffer.from(JSON.stringify(body), 'utf8');
    const headers = { 'Content-Type': 'application/json', 'Content-Length': data.length, 'User-Agent': userAgent };
    if (token) headers.Authorization = `Bearer ${token}`;
    const req = mod.request(new URL(path, base), { method: 'POST', headers, timeout: TIMEOUT_MS }, (res) => {
      const chunks = [];
      let size = 0;
      res.on('data', (c) => {
        size += c.length;
        if (size > MAX_RESPONSE) { req.destroy(new Error('příliš velká odpověď serveru')); return; }
        chunks.push(c);
      });
      res.on('end', () => {
        let json = {};
        try { json = JSON.parse(Buffer.concat(chunks).toString('utf8') || '{}'); } catch (e) { json = {}; }
        resolve([res.statusCode, json]);
      });
    });
    req.on('timeout', () => req.destroy(new Error('server neodpověděl včas')));
    req.on('error', reject);
    req.end(data);
  });
}

module.exports = { makePost };
