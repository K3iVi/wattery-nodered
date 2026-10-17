# node-red-contrib-wattery

Lokální spojení **Victron GX** (Cerbo GX, Ekrano GX) s platformou **wattery** — přímo z Node-RED na Venus OS Large.
Data jdou z Cerba každých pár sekund místo přes VRM. Verze 0.1 **jen čte**, do Cerba nic nezapisuje.

## Instalace (Venus OS Large)

1. Cerbo: Nastavení → Obecné → Firmware → Online aktualizace → Typ image **Large** → nainstalovat.
2. Nastavení → Integrace → **Node-RED** → zapnout. Otevřít `https://venus.local:1881` (nebo VRM → Venus OS Large).
3. Menu → **Spravovat paletu** → Instalovat → hledat `wattery` → Instalovat.
4. Přetáhnout uzel **wattery** na plochu, vložit párovací kód z aplikace wattery („Připojit lokálně“) → **Nasadit**.

MQTT v síti ani heslo Cerba zapínat/zadávat není potřeba — uzel čte broker na `localhost`.

## Vývoj

```
npm test                       # jednotkové testy (falešný GX, falešný server)
python tools/e2e.py            # E2E proti hubu deye-monitor a simulátoru GX (sourozenecká repa)
node tools/headless.js --url … --code … --host … --port …   # jádro bez Node-RED
```

Jádro (`lib/core.js`, `lib/gx-mqtt.js`) je port čtecí části klienta pro Home Assistant
(`wattery-ha/custom_components/wattery/edge.py`, `transport_mqtt_gx.py`) — držet stejně.

MIT
