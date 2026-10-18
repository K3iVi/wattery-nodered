# node-red-contrib-wattery

Lokální spojení **Victron GX** (Cerbo GX, Ekrano GX) s aplikací **[wattery](https://spot.wattery.cz)** — přímo
z Node-RED na Venus OS Large. Hodnoty jdou z Cerba každých pár sekund po vaší síti, ne přes VRM.

- **Jen čte.** Do Cerba ani do zařízení Victron nic nezapisuje (verze 0.1).
- **Nic dalšího nenastavujete:** uzel čte broker MQTT přímo v Cerbu (`localhost`), MQTT v síti ani heslo Cerba
  nepotřebuje.
- **Bez závislostí**, malé: Node-RED s uzlem zabere na Cerbu kolem 105 MB paměti.

**Návod s obrázky:** [spot.wattery.cz/navody/pripojeni-victron-cerbo](https://spot.wattery.cz/navody/pripojeni-victron-cerbo)

## Instalace

1. **Venus OS Large:** Settings → General → Firmware → Online updates → Image type **Large** → aktualizovat.
2. **Node-RED:** Settings → Integrations → Node-RED → **Enabled**. Otevřete `https://<adresa Cerba>:1881`
   (nebo ve VRM: Venus OS Large → Node-RED).
3. V Node-RED: menu ☰ → **Manage palette** → **Install** → hledat `wattery` → **Install**.
4. Ve wattery: **Nastavení → Lokální spojení → Spárovat zařízení** — ukáže se kód (platí 15 minut).
5. Uzel **wattery** (kategorie wattery) přetáhněte na plochu, dvakrát klikněte, vložte kód → **Done** → **Deploy**.
   Pod uzlem se objeví zelené **čte · hh:mm** a ve wattery **Node-RED na Cerbu ● online**.

## Stavy uzlu

| Stav | Co znamená |
|---|---|
| zelená **čte · 14:05** | vše v pořádku, čas poslední synchronizace |
| žlutá **zadejte párovací kód** | uzel ještě není spárovaný — vložte kód z aplikace |
| žlutá **server nedostupný** | Cerbo nemá internet nebo server neodpovídá; zkouší znovu samo |
| žlutá **GX: … zkusím znovu** | spojení s Cerbem (MQTT) se přerušilo; za 10 s znovu |
| červená **odpojeno ve wattery — spárujte znovu** | spojení bylo v aplikaci odpojeno; vložte nový kód a Deploy |

## Odebrání

Smažte uzel a dejte **Deploy**, ve wattery u spojení klepněte na **✕**. Balíček odeberete v Manage palette.

## English

Read-only bridge from a Victron GX device (Venus OS Large, Node-RED) to the wattery energy platform. It reads the
GX's local MQTT broker on `localhost` (no MQTT-on-LAN or GX password needed) and syncs the values to wattery over
HTTPS. Pair it with a one-time code from the wattery app. Nothing is ever written to the GX (only the
`R/<portal>/keepalive` request is published).

## Vývoj

```
npm test                       # jednotkové testy (falešný GX, falešný server)
python tools/e2e.py            # E2E proti hubu deye-monitor a simulátoru GX (sourozenecká repa)
node tools/headless.js --url … --code … --host … --port …   # jádro bez Node-RED
```

Jádro (`lib/core.js`, `lib/gx-mqtt.js`) je port čtecí části klienta pro Home Assistant
([wattery-ha](https://github.com/K3iVi/wattery-ha): `edge.py`, `transport_mqtt_gx.py`) — držet stejně.

Licence MIT
