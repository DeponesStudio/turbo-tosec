# 🚀 turbo-tosec

> **TOSEC veritabanlarını ışık hızında sorgulamak için DuckDB tabanlı yüksek performanslı importer.**

**turbo-tosec**, devasa **TOSEC (The Old School Emulation Center)** DAT koleksiyonlarını tarar, ayrıştırır (parse) ve saniyeler içinde sorgulanabilir, tek parça bir **DuckDB** veritabanına dönüştürür.

Arşivciler ve retro oyun tutkunları için; yüz binlerce dosyalık XML/DAT yığınlarını, SQL ile anında sorgulanabilir modern bir veriye dönüştürür.

## ⚡ Neden turbo-tosec?

- **Hız Odaklı:** Python'un XML parsing gücünü DuckDB'nin "Bulk Insert" yeteneğiyle birleştirir.
- **Sıfır Bağımlılık:** Harici bir sunucu (MySQL, Postgres) gerektirmez. Tek çıktı `.duckdb` dosyasıdır.
- **Akıllı Tarama:** Alt klasörlerdeki binlerce `.dat` dosyasını otomatik bulur (`recursive scan`).
- **İlerleme Takibi:** `tqdm` ile detaylı, canlı işlem durumu gösterir.

## 📦 Kurulum

Bu proje Python 3.x gerektirir.

```bash
git clone [https://github.com/KULLANICI_ADINIZ/turbo-tosec.git](https://github.com/berkacunas/turbo-tosec.git)
cd turbo-tosec
pip install -r requirements.txt
```

## 🛠️ Kullanım

### 1\. Veriyi Hazırlayın

Bu araç, TOSEC DAT dosyalarını işler. En güncel DAT paketini [TOSEC Resmi Sitesinden](https://www.tosecdev.org/downloads) indirin ve bir klasöre çıkarın.

### 2\. Çalıştırın

#### Standart Mod (Güvenli)
Hata ayıklama (debugging) veya küçük koleksiyonlar için en iyisidir. Tek bir iş parçacığı (single thread) kullanır.
```bash
python tosec_importer.py -i "/dosya/yolu/TOSEC" -o "tosec.duckdb"
```

#### Turbo Mod (Çok İş Parçacıklı) 🔥

İşlemcinizin tüm gücünü serbest bırakın\! Tam TOSEC arşivini içe aktarmak için önerilir.

```bash
# 8 işçi thread ve daha büyük işlem (batch) boyutu kullanımı
python tosec_importer.py -i "/dosya/yolu/TOSEC" -w 8 -b 5000
```

#### Komut Satırı Argümanları

| Parametre | Açıklama | Varsayılan |
| :--- | :--- | :--- |
| `-i, --input` | DAT dosyalarını içeren kök dizinin yolu. | **Zorunlu** |
| `-o, --output` | Oluşturulacak DuckDB veritabanı dosyasının yolu. | `tosec.duckdb` |
| `-w, --workers` | Paralel ayrıştırma için kullanılacak iş parçacığı sayısı. | `1` |
| `-b, --batch-size`| Her veritabanı işleminde (transaction) eklenecek kayıt sayısı. | `1000` |
| `--no-open-log` | Hata oluştuğunda log dosyasını otomatik olarak **açma**. | `False` |

## 🔍 Örnek Sorgular (DuckDB / SQL)

Oluşturulan veritabanını **DBeaver**, **VSCode SQLTools** veya **Python** ile açıp şu sorguları atabilirsiniz:

**Doğrulanmış [\!] Commodore 64 Oyunlarını Bul:**

```sql
SELECT game_name, rom_name 
FROM roms 
WHERE platform LIKE '%Commodore 64%' 
  AND rom_name LIKE '%[!]%';
```

**Elimdeki Dosyanın Orjinalliğini Kontrol Et (Hash ile):**

```sql
SELECT * FROM roms WHERE md5 = 'DOSYANIZIN_MD5_HASH_DEGERI';
```

## 📄 Lisans

Bu proje [MIT Lisansı](https://choosealicense.com/licenses/mit/) altında lisanslanmıştır.
*Not: Bu proje TOSEC veritabanı dosyalarını içermez, sadece bu dosyaları işlemek için bir araç sağlar.*

**Copyright © 2025 berkacunas & DeponesStudio.**