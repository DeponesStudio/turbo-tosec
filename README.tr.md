# 🚀 turbo-tosec v2.0

> **DuckDB & Apache Arrow Destekli Yüksek Performanslı TOSEC Veri İşleme Motoru.**

**turbo-tosec**, kapsamlı **TOSEC (The Old School Emulation Center)** DAT koleksiyonlarını taramak, ayrıştırmak ve sorgulanabilir tek bir **DuckDB** veritabanı dosyasına dönüştürmek için tasarlanmış bir veri mühendisliği aracıdır.

Geleneksel XML ayrıştırıcıların aksine **turbo-tosec v2.0**, gigabytelarce büyüklükteki metaveriyi saniyeler içinde işlemek için modern **Sıfır Kopya (Zero-Copy Ingestion)** ve **ETL (Extract-Transform-Load)** tekniklerini kullanır. Dağınık XML dosyalarını yapılandırılmış bir SQL veri ambarına dönüştürür.

---

### 📥 Kurulumsuz Kullanım (Standalone Executable)

Python kurulumuna ihtiyaç duymadan, işletim sisteminize uygun derlenmiş sürümü kullanabilirsiniz:

* **Windows:** [İndir: `turbo-tosec_v2.0.0_Windows.exe](https://www.google.com/search?q=%5Bhttps://github.com/berkacunas/turbo-tosec/releases/latest%5D(https://github.com/berkacunas/turbo-tosec/releases/latest))`
* **Linux:** [İndir: `turbo-tosec_v2.0.0_Linux.tar.gz](https://www.google.com/search?q=%5Bhttps://github.com/berkacunas/turbo-tosec/releases/latest%5D(https://github.com/berkacunas/turbo-tosec/releases/latest))`

---

## ⚡ Temel Özellikler

* **Üç Farklı İşleme Stratejisi:** Donanım kısıtlarına ve veri boyutuna göre **Direct**, **Staged** veya **Legacy** modları seçilebilir.
* **Kesinti Toleransı (Crash-Safe):** Elektrik kesintisi veya sistem hatası durumunda, **Staged Mode** işlemi diske kaydeder ve bir sonraki çalıştırmada kaldığı yerden devam eder (Resume Capability).
* **Bağımsız Mimari:** Harici bir veritabanı sunucusuna (MySQL, Postgres vb.) ihtiyaç duymaz. Çıktı, taşınabilir bir `.duckdb` dosyasıdır.
* **Apache Arrow Entegrasyonu:** Python ve DuckDB arasındaki veri transferinde sütun bazlı bellek formatı kullanılarak işlemci maliyeti minimize edilir.
* **Rekürsif Tarama:** İç içe geçmiş klasör yapılarındaki binlerce `.dat` dosyasını otomatik olarak tespit eder.

## 📦 Kurulum

Bu proje Python 3.9 ve üzeri sürümleri gerektirir.

```bash
git clone https://github.com/berkacunas/turbo-tosec.git
cd turbo-tosec
pip install .

```

## 🛠️ Kullanım ve Stratejiler

**turbo-tosec**, veri işleme (ingestion) süreci için üç farklı strateji sunar:

### 1. Direct Mode (Streaming)

**Önerilen Senaryo:** Yüksek Hız, Yeterli RAM, SSD Disk.

XML verisini okur ve **Apache Arrow** kullanarak disk üzerinde ara işlem yapmadan doğrudan DuckDB'ye yazar (Stream). En yüksek işlem hacmine (throughput) sahip moddur.

```bash
turbo-tosec --input "C:\TOSEC\DATs" --direct

```

### 2. Staged Mode (Batch / ETL)

**Önerilen Senaryo:** Çok Büyük Veri Setleri, Düşük RAM, Veri Güvenliği.

Klasik **ETL** prensibini uygular. XML verisi önce sıkıştırılmış geçici **Parquet** dosyalarına dönüştürülür (Staging), ardından toplu olarak veritabanına yüklenir.

* **Devam Edebilirlik:** İşlem yarıda kesilirse, tekrar çalıştırıldığında işlenmiş dosyalar atlanır.
* **Paralel İşleme:** Çok çekirdekli işlemcilerde `workers` parametresi ile hızlandırılabilir.

```bash
# 4 işlemci çekirdeği ile çalıştırma örneği
turbo-tosec --input "C:\TOSEC\DATs" --staged --workers 4

```

### 3. In-Memory Mode (Legacy)

**Önerilen Senaryo:** Küçük dosyalar ve hata ayıklama.

Tüm XML ağacını (DOM) belleğe yükler. Büyük dosyalar için bellek yönetimi açısından verimsizdir. Herhangi bir mod belirtilmezse varsayılan olarak bu mod çalışır.

```bash
turbo-tosec --input "C:\TOSEC\DATs"

```

## ⚙️ Parametreler (CLI)

| Parametre | Açıklama | Varsayılan |
| --- | --- | --- |
| `-i, --input` | DAT dosyalarını içeren kök dizin yolu. | **Zorunlu** |
| `-o, --output` | Çıktı veritabanı dosyasının yolu. | `tosec.duckdb` |
| `--direct` | Sıfır Kopya Akış Modunu (Zero-Copy Streaming) etkinleştirir. | `False` |
| `--staged` | Aşamalı ETL Modunu (Batch Processing) etkinleştirir. | `False` |
| `-w, --workers` | Paralel işlem sayısı (Sadece Staged Mode). | `CPU Sayısı` |
| `--temp-dir` | Geçici Parquet dosyaları için dizin (Staged Mode). | `temp_chunks` |
| `-b, --batch-size` | Veritabanı işlem (transaction) boyutu. | `1000` |

## ⚡ Performans Testleri

*Yaklaşık 3.000 DAT dosyası ve 1 Milyon+ ROM girdisi içeren veri seti üzerinde test edilmiştir.*

| Strateji | Hız | RAM Kullanımı | Disk I/O |
| --- | --- | --- | --- |
| **In-Memory** | Yavaş | Yüksek | Düşük |
| **Staged** | Hızlı | Düşük | Yüksek (Geçici Dosya) |
| **Direct** | **En Hızlı** | Düşük | **Minimal** |

## 🔍 Örnek Sorgular (SQL)

Oluşturulan `.duckdb` dosyası **DBeaver** veya **VSCode SQLTools** kullanılarak sorgulanabilir.

**Doğrulanmış [!] Commodore 64 Oyunlarını Listeleme:**

```sql
SELECT game_name, rom_name 
FROM roms 
WHERE platform LIKE '%Commodore 64%' 
  AND rom_name LIKE '%[!]%';

```

**Mükerrer Kayıt (Clone) Analizi:**

```sql
SELECT crc, COUNT(*) as count 
FROM roms 
GROUP BY crc 
HAVING count > 1 
ORDER BY count DESC;

```

## 📚 Dokümantasyon

Mimari detaylar ve ileri seviye kullanım senaryoları için **[Proje Wiki](https://github.com/berkacunas/turbo-tosec/wiki)** sayfasını inceleyebilirsiniz.

## 📄 Lisans

Bu proje **GNU General Public License v3.0 (GPL-3.0)** altında lisanslanmıştır.

---

*Yasal Uyarı: Bu proje TOSEC veritabanı dosyalarını veya ROM dosyalarını içermez. Sadece TOSEC projesi tarafından sağlanan metaveri dosyalarını işlemek için teknik bir araç sağlar.*

**Telif Hakkı © 2025 berkacunas & Depones Labs.**