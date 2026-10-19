# node-red-contrib-wattery

Lokální spojení **Victron GX** (Cerbo GX, Ekrano GX) s aplikací **[wattery](https://spot.wattery.cz)** — přímo
z Node-RED na Venus OS Large. Hodnoty jdou z Cerba každých pár sekund po vaší síti, ne přes VRM.

- **Výchozí stav: jen čte.** Do Cerba nic nezapisuje, dokud v uzlu nezapnete **Řízení z wattery** (viz níže).
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

## Řízení z wattery (od verze 0.2)

Volba **Řízení z wattery (zápis do Cerba)** v uzlu je ve výchozím stavu **vypnutá**. Po zapnutí může wattery
přes uzel nastavovat jen to, co povoluje profil `victron-gx@1`: rozvrh Dynamic ESS (sloty, Mode 0/4), minimální SOC
baterie a nabíječky EV. Nic jiného uzel do Cerba nezapíše — ani když by o to server požádal.

- Řízení musí být zapnuté **na obou stranách**: v uzlu i ve wattery u spojení.
- Každý zápis uzel ověří zpětným přečtením z Cerba; wattery dostane jen ověřený výsledek.
- Limity zápisů (za minutu, za hodinu, trvalá nastavení za den) a mezera mezi zápisy jsou v profilu.
- **Pojistka:** když se uzel se serverem wattery dlouho nespojí (dnes 1 hodinu), sám vypne Dynamic ESS
  (Mode 0) — Cerbo pak jede podle vašeho vlastního nastavení ESS — a po obnovení spojení to wattery nahlásí.
- Vypnutím volby a **Deploy** uzel okamžitě přestane do Cerba zapisovat. Rozvrh, který už v Cerbu je, doběhne;
  Dynamic ESS můžete kdykoli vypnout přímo v Cerbu (nastavení ESS → Dynamic ESS).

## Stavy uzlu

| Stav | Co znamená |
|---|---|
| zelená **čte · 14:05** | vše v pořádku, čas poslední synchronizace (řízení vypnuté) |
| zelená **řízení · 14:05** | vše v pořádku, řízení z wattery zapnuté |
| žlutá **zadejte párovací kód** | uzel ještě není spárovaný — vložte kód z aplikace |
| žlutá **server nedostupný** | Cerbo nemá internet nebo server neodpovídá; zkouší znovu samo |
| žlutá **GX: … zkusím znovu** | spojení s Cerbem (MQTT) se přerušilo; za 10 s znovu |
| červená **odpojeno ve wattery — spárujte znovu** | spojení bylo v aplikaci odpojeno; vložte nový kód a Deploy |

## Odebrání

Smažte uzel a dejte **Deploy**, ve wattery u spojení klepněte na **✕**. Balíček odeberete v Manage palette.

## English

Bridge from a Victron GX device (Venus OS Large, Node-RED) to the wattery energy platform. It reads the
GX's local MQTT broker on `localhost` (no MQTT-on-LAN or GX password needed) and syncs the values to wattery over
HTTPS. Pair it with a one-time code from the wattery app. By default it is read-only (only the
`R/<portal>/keepalive` request is published). With the opt-in **control** option (0.2+) wattery may write the
paths allowed by the `victron-gx@1` profile (Dynamic ESS schedule, minimum SOC, EV chargers); every write is
verified by read-back and rate-limited; if the node loses the wattery server for too long (1 hour today), it switches
Dynamic ESS off (Mode 0) so the GX falls back to the owner's own ESS settings.

## Vývoj

```
npm test                       # jednotkové testy (falešný GX, falešný server)
python tools/e2e.py            # E2E proti hubu deye-monitor a simulátoru GX (sourozenecká repa)
python tools/e2e_control.py    # E2E řízení: zápis W/ → ozvěna → pojistka po výpadku serveru
python tools/gen_vectors.py    # zlaté vektory profilu z Pythonu (wattery-ha write_profile)
node tools/headless.js --url … --code … --host … --port …   # jádro bez Node-RED
```

Jádro (`lib/core.js`, `lib/gx-mqtt.js`, `lib/profile.js`) je port klienta pro Home Assistant
([wattery-ha](https://github.com/K3iVi/wattery-ha): `edge.py`, `transport_mqtt_gx.py`, `write_profile.py`) —
držet stejně. Profil `profiles/victron-gx@1.json` musí být bajtově stejný jako v wattery-ha (hlídá test).

Licence MIT
