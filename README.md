# HA to DB

Doplněk (add-on) pro Home Assistant, který v pravidelném intervalu – typicky
každých 5 minut – zapisuje aktuální hodnoty vybraných entit do databáze.
Funguje stejně jako statistiky v LoxBerry (plugin Stats4Lox) a umí zapisovat
do stejné InfluxDB ve stejném tvaru.

- výběr entit zaškrtnutím v seznamu přímo v Home Assistantovi
- InfluxDB 1.x, MariaDB/MySQL, PostgreSQL, MSSQL, SQLite
- při výpadku databáze se hodnoty podrží v paměti a zapíší později
- přihlašovací údaje se zadávají jen v konfiguraci doplňku, v repozitáři žádné nejsou

## Instalace

1. Nastavení → Doplňky → Obchod s doplňky → ⋮ → **Repozitáře** a přidejte adresu
   tohoto repozitáře.
2. Nainstalujte **HA to DB**, na kartě *Konfigurace* vyplňte přístup k databázi
   a doplněk spusťte.
3. Přes **Otevřít webové rozhraní** zaškrtněte entity, které se mají ukládat.

Podrobnosti jsou v [ha2db/DOCS.md](ha2db/DOCS.md).
