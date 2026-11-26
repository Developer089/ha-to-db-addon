# HA to DB

Doplněk v pravidelném intervalu (výchozí 5 minut) přečte aktuální stavy vybraných
entit z Home Assistanta a zapíše je do databáze – do InfluxDB 1.x (stejně jako
LoxBerry Stats4Lox) nebo do SQL databáze.

## Instalace (lokální doplněk)

1. Zkopírujte složku `ha2db` do `/addons/ha2db` na stroji s Home Assistantem
   (přes doplněk Samba nebo SSH).
2. Nastavení → Doplňky → Obchod s doplňky → ⋮ → *Zkontrolovat aktualizace*.
3. V sekci *Local add-ons* nainstalujte **HA to DB**, vyplňte konfiguraci a spusťte.

## Výběr hodnot

Otevřete doplněk tlačítkem **Otevřít webové rozhraní** (nebo si ho zapněte v
postranním panelu). Zobrazí se seznam všech entit Home Assistanta s aktuální
hodnotou – zaškrtnuté se zapisují do databáze. Seznam lze prohledávat, filtrovat
podle typu a hromadně vybrat nebo zrušit vše zobrazené. Změna se ukládá hned a
projeví se při nejbližším zápisu, restart není potřeba.

Entity označené „pravidlo v konfiguraci“ jsou vybrané přes `include_domains` /
`include_entities` a odškrtnout je lze jen tam.

## Konfigurace

| Volba | Význam |
|---|---|
| `db_type` | `influxdb`, `mariadb`, `mysql`, `postgresql`, `mssql`, `sqlite` |
| `db_host`, `db_port` | adresa DB serveru (InfluxDB 8086, MariaDB 3306) |
| `db_name` | název databáze; u `sqlite` cesta k souboru, např. `/share/ha2db.sqlite` |
| `db_user`, `db_password` | přihlašovací údaje |
| `interval_seconds` | interval zápisu (300 = 5 minut) |
| `align_to_clock` | zápis v „kulatých“ časech (:00, :05, :10 …) |
| `include_domains` | volitelné: domény, které se zapisují celé (např. `sensor`) |
| `include_entities` | volitelné: entity podle vzoru s `*` (např. `switch.cerpadlo*`) |
| `exclude_entities` | entity, které se nezapisují nikdy a v seznamu se nezobrazí |
| `numeric_only` | zapisovat jen číselné hodnoty |
| `skip_unavailable` | přeskočit stavy `unavailable` / `unknown` |
| `log_level` | podrobnost logu |

Jen pro InfluxDB:

| Volba | Význam |
|---|---|
| `influx_ssl` | připojení přes HTTPS (LoxBerry: ano) |
| `influx_verify_ssl` | ověřovat certifikát (LoxBerry má vlastní podepsaný: ne) |
| `influx_measurement` | název measurementu: `friendly_name` nebo `entity_id` |
| `influx_measurement_prefix` | text před názvem measurementu, např. `HA ` |

Jen pro SQL:

| Volba | Význam |
|---|---|
| `table_name` | tabulka, vytvoří se sama |
| `retention_days` | mazat záznamy starší než N dní (0 = nemazat) |

## InfluxDB na LoxBerry (Stats4Lox)

Pro InfluxDB pluginu Stats4Lox nastavte `db_type: influxdb`, port 8086 a
`influx_ssl` zapnuté. Adresu, název databáze, uživatele a heslo vyplňte podle
své instalace (viz *Přihlašovací údaje* níže).

Data se ukládají ve stejném tvaru, jaký používá Stats4Lox, takže jdou v Grafaně
zobrazit stejným způsobem:

| Část | Obsah |
|---|---|
| measurement | zobrazovaný název entity (případně s prefixem) |
| pole `Default` | číselná hodnota |
| pole `state` | textový stav, pokud hodnota není číslo |
| tag `name` | zobrazovaný název |
| tag `source` | vždy `homeassistant` (Stats4Lox má `grabber` / `import`) |
| tag `entity_id` | např. `sensor.teplota_obyvak` |
| tag `type` | doména entity (`sensor`, `switch` …) |
| tag `category` | `device_class` entity (`temperature`, `power` …) |
| tag `unit` | jednotka |

Pokud se entita v HA jmenuje stejně jako statistika z Loxone, skončí obě ve
stejném measurementu a liší se jen tagem `source`. Chcete-li je mít oddělené,
nastavte `influx_measurement_prefix`.

## SQL tabulka

| Sloupec | Obsah |
|---|---|
| `id` | automatické číslo |
| `ts` | čas zápisu v **UTC** |
| `entity_id` | např. `sensor.teplota_obyvak` |
| `value` | číselná hodnota, `NULL` pokud stav není číslo |
| `state` | stav jako text, jak ho ukazuje HA |
| `unit` | jednotka |
| `friendly_name` | zobrazovaný název |

Databáze i uživatel musí existovat předem, uživatel potřebuje práva
`CREATE`, `INSERT` a (při retenci) `DELETE`. U MariaDB na jiném stroji musí mít
uživatel povolené přihlášení ze sítě:

```sql
CREATE DATABASE IF NOT EXISTS ha CHARACTER SET utf8mb4;
CREATE USER 'ha2db'@'%' IDENTIFIED BY 'heslo';
GRANT CREATE, INSERT, DELETE, INDEX ON ha.* TO 'ha2db'@'%';
```

## Přihlašovací údaje

Adresa serveru, název databáze, uživatel a heslo se zadávají jen v Home
Assistantovi na kartě **Konfigurace** doplňku a lze je tam kdykoli změnit
(po uložení doplněk restartujte). Ukládají se pouze na stroji s Home
Assistantem, ve zdrojových souborech doplňku žádné nejsou – výchozí hodnoty
jsou prázdné a bez vyplnění se doplněk nespustí.

Heslo lze místo přímého zápisu vzít ze `secrets.yaml` Home Assistanta:
do pole `db_password` napište `!secret influx_password`.

## Výpadek databáze

Když je databáze nedostupná, řádky zůstávají v paměti (max. 200 000) a zapíší se
při dalším cyklu. Restart doplňku tuto frontu zahodí.
