# 🚀 turbo-tosec v2.0

> **DuckDB & Apache Arrow Destekli Yüksek Performanslı TOSEC Veri İşleme Motoru.**

**turbo-tosec**, kapsamlı **TOSEC (The Old School Emulation Center)** DAT koleksiyonlarını taramak, ayrıştırmak ve sorgulanabilir tek bir **DuckDB** veritabanı dosyasına dönüştürmek için tasarlanmış bir veri mühendisliği aracıdır.

Geleneksel XML ayrıştırıcıların aksine **turbo-tosec v2.0**, gigabytelarce büyüklükteki metaveriyi saniyeler içinde işlemek için modern **Sıfır Kopya (Zero-Copy Ingestion)** ve **ETL (Extract-Transform-Load)** tekniklerini kullanır. Dağınık XML dosyalarını yapılandırılmış bir SQL veri ambarına dönüştürür.

---

### 📥 Kurulumsuz Kullanım (Standalone Executable)

Python kurulumuna ihtiyaç duymadan, işletim sisteminize uygun derlenmiş sürümü kullanabilirsiniz:

* **Windows:** [İndir: `turbo-tosec_v2.0.0_Windows.exe](https://github.com/deponeslabs/turbo-tosec/releases/latest%5D(https://github.com/deponeslabs/turbo-tosec/releases/latest))`
* **Linux:** [İndir: `turbo-tosec_v2.0.0_Linux.tar.gz](https://github.com/deponeslabs/turbo-tosec/releases/latest%5D(https://github.com/deponeslabs/turbo-tosec/releases/latest))`

  * **Windows:** [`turbo-tosec_v2.0.0_Windows.exe` İndir](https://github.com/deponeslabs/turbo-tosec/releases/latest%5D\(https://github.com/berkacunas/turbo-tosec/releases/latest\))
  * **Linux:** [`turbo-tosec_v2.0.0_Linux.tar.gz` İndir](https://github.com/deponeslabs/turbo-tosec/releases/latest%5D\(https://github.com/berkacunas/turbo-tosec/releases/latest\))

## ⚡ Temel Özellikler

* **Akıllı Varsayılan Strateji:** Karmaşık konfigürasyona ihtiyaç duymadan, veri bütünlüğü için en güvenli yöntemi (Staged Mode) otomatik seçer.
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

### 1. Staged Mode (Varsayılan / Önerilen) 🛡️

**Senaryo:** Büyük Veri Setleri, Veri Güvenliği, Kesinti Toleransı.

Bu, programın **varsayılan davranışıdır**. Klasik **ETL** prensibini uygular. XML verisi önce sıkıştırılmış geçici **Parquet** dosyalarına dönüştürülür, ardından toplu olarak veritabanına yüklenir.

* **Devam Edebilirlik:** İşlem yarıda kesilirse, tekrar çalıştırıldığında işlenmiş dosyalar atlanır.
* **Stabilite:** Bellek dalgalanmalarını (RAM Spikes) minimize eder.
* **Paralel İşleme:** Çok çekirdekli işlemcilerde `workers` parametresi ile hızlandırılabilir.

```bash
# Doğrudan çalıştırın. Staged mod otomatiktir.
turbo-tosec --input "C:\TOSEC\DATs"

# İsteğe bağlı: İşlemci çekirdek sayısını elle belirtme
turbo-tosec --input "C:\TOSEC\DATs" --workers 4

```

### 2. Direct Mode (Streaming) 🏎️

**Senaryo:** Yüksek Hız, Yeterli RAM, SSD Disk.

XML verisini okur ve **Apache Arrow** kullanarak disk üzerinde ara işlem yapmadan doğrudan DuckDB'ye yazar (Stream). En yüksek işlem hacmine (throughput) sahip moddur.

```bash
turbo-tosec --input "C:\TOSEC\DATs" --direct

```

### 3. In-Memory Mode (Legacy) 💾

**Senaryo:** Küçük dosyalar ve hata ayıklama (Debugging).

Eski yöntemdir. Tüm XML ağacını (DOM) belleğe yükler. Büyük dosyalar için bellek yönetimi açısından verimsizdir ve **önerilmez**.

```bash
turbo-tosec --input "C:\TOSEC\DATs" --legacy

```

## ⚙️ Parametreler (CLI)

| Parametre | Açıklama | Varsayılan |
| --- | --- | --- |
| `-i, --input` | DAT dosyalarını içeren kök dizin yolu. | **Zorunlu** |
| `-o, --output` | Çıktı veritabanı dosyasının yolu. | `tosec.duckdb` |
| `--staged` | Aşamalı ETL Modunu açıkça belirtir (Varsayılan davranış). | `True` (Örtük) |
| `--direct` | Sıfır Kopya Akış Modunu (Streaming) etkinleştirir. | `False` |
| `--legacy` | Kullanımdan kalkan In-Memory DOM Modunu etkinleştirir. | `False` |
| `-w, --workers` | Paralel işlem sayısı (Sadece Staged Mode). | `CPU Sayısı` |
| `--temp-dir` | Geçici Parquet dosyaları için dizin. | `temp_chunks` |
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

Mimari detaylar ve ileri seviye kullanım senaryoları için **[Proje Wiki](https://github.com/deponeslabs/turbo-tosec/wiki)** sayfasını inceleyebilirsiniz.

## 📄 Lisans

Bu proje **GNU General Public License v3.0 (GPL-3.0)** altında lisanslanmıştır.

---

*Yasal Uyarı: Bu proje TOSEC veritabanı dosyalarını veya ROM dosyalarını içermez. Sadece TOSEC projesi tarafından sağlanan metaveri dosyalarını işlemek için teknik bir araç sağlar.*

**Telif Hakkı © 2025 Depones Labs.**