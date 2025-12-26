# 🚀 turbo-tosec v2.0

> **DuckDB & Apache Arrow Destekli Yüksek Performanslı TOSEC Veri İşleme Motoru.**

**turbo-tosec**, kapsamlı **TOSEC (The Old School Emulation Center)** DAT koleksiyonlarını taramak, ayrıştırmak ve sorgulanabilir tek bir **DuckDB** veritabanı dosyasına dönüştürmek için tasarlanmış yeni nesil bir veri mühendisliği aracıdır.

Geleneksel XML ayrıştırıcıların aksine **turbo-tosec v2.0**, gigabytelarce büyüklükteki metaveriyi saniyeler içinde işlemek için modern **Sıfır Kopya (Zero-Copy Ingestion)** ve **ETL Staging** tekniklerini kullanır. Dağınık XML dosyalarını yapılandırılmış bir SQL veri ambarına dönüştürür.

---

### 📥 İndir (Python Gerekmez)

Python kurulumuna ihtiyaç duymadan, işletim sisteminize uygun derlenmiş sürümü (standalone executable) indirip kullanabilirsiniz:

* **Windows:** [İndir `turbo-tosec_v2.1.2_Windows.exe`](https://github.com/deponeslabs/turbo-tosec/releases/latest)
* **Linux:** [İndir `turbo-tosec_v2.1.2_Linux.tar.gz`](https://github.com/deponeslabs/turbo-tosec/releases/latest)

---

## ⚡ Neden turbo-tosec v2.0?

* **Akıllı Varsayılan Strateji:** Karmaşık konfigürasyona ihtiyaç duymadan, en güvenli veri işleme yöntemini (Staged Mode) otomatik seçer.
* **Kesinti Toleransı (Crash-Safe):** Elektrik kesintisi mi oldu? Sorun değil. **Staged Mode**, ilerlemeyi diske kaydeder ve işlem tekrar başlatıldığında tam olarak kaldığı yerden devam eder.
* **Bağımsız Mimari:** MySQL veya Postgres sunucularına ihtiyaç duymaz. Çıktı, taşınabilir tek bir `.duckdb` dosyasıdır.
* **Apache Arrow Entegrasyonu:** Python ve DuckDB arasındaki veri transferinde sütun bazlı bellek formatı kullanılarak işlem şimşek hızında tamamlanır (Direct Mode).
* **Akıllı Rekürsif Tarama:** İç içe geçmiş alt dizinlerdeki binlerce `.dat` dosyasını otomatik olarak avlar.

## 📦 Kurulum

Bu proje Python 3.9 ve üzeri sürümleri gerektirir.

```bash
git clone https://github.com/berkacunas/turbo-tosec.git
cd turbo-tosec
pip install .

```

## 🛠️ Kullanım ve Stratejiler

**turbo-tosec**, veri işleme (ingestion) süreci için ihtiyacınıza uygun farklı stratejiler sunar:

### 1. Staged Mode (Varsayılan / Önerilen) 🛡️

**Senaryo:** Devasa Veri Setleri, Güvenilirlik, Kesinti Toleransı.

Bu, programın **varsayılan davranışıdır**. **ETL (Extract, Transform, Load)** desenini izler. XML verilerini toplu yüklemeden önce sıkıştırılmış geçici Parquet dosyalarına ayrıştırır.

* **Devam Edebilirlik:** İşlem yarıda kesilirse, komutu tekrar çalıştırmak işlenmiş dosyaları atlayarak devam etmeyi sağlar.
* **Güvenli:** RAM kullanımındaki ani yükselmeleri (spikes) minimize eder.

```bash
# Sadece çalıştırın. Staged mod otomatiktir.
turbo-tosec --input "C:\TOSEC\DATs"

# İsteğe bağlı: İşlemci çekirdek sayısını (worker) elle belirtebilirsiniz
turbo-tosec --input "C:\TOSEC\DATs" --workers 4

```

### 2. Direct Mode (Streaming) 🏎️

**Senaryo:** Yüksek Hız, İyi RAM, Hızlı SSD Diskler.

Disk üzerinde ara işlem yapmadan XML verisini **Apache Arrow** kullanarak doğrudan DuckDB'ye akıtır (Stream). En hızlı yöntemdir (Sıfır Kopya) ancak hata toleransı Staged Mode'a göre daha düşüktür.

```bash
turbo-tosec --input "C:\TOSEC\DATs" --direct

```

### 3. In-Memory Mode (Legacy) 💾

**Senaryo:** Çok küçük dosyalar veya hata ayıklama.

Eski yöntemdir. Tüm XML ağacını (DOM) belleğe yükler. **Kullanımdan kaldırılmıştır (Deprecated)** ve büyük dosyalar için önerilmez.

```bash
turbo-tosec --input "C:\TOSEC\DATs" --legacy

```

## ⚙️ CLI Argümanları

| Bayrak | Açıklama | Varsayılan |
| --- | --- | --- |
| `-i, --input` | DAT dosyalarını içeren kök dizin yolu. | **Zorunlu** |
| `-o, --output` | Çıktı veritabanı dosyasının yolu. | `tosec.duckdb` |
| `--staged` | ETL Batch Modunu açıkça belirtir (Varsayılan davranış). | `True` (Örtük) |
| `--direct` | Sıfır Kopya Akış Modunu (En Hızlı) etkinleştirir. | `False` |
| `--legacy` | Kullanımdan kalkan In-Memory DOM Modunu etkinleştirir. | `False` |
| `-w, --workers` | Paralel işlem sayısı (Staged Mode). | `CPU Sayısı` |
| `--temp-dir` | Geçici Parquet parçaları için dizin. | `temp_chunks` |
| `-b, --batch-size` | Veri ekleme işlemleri için parti boyutu. | `1000` |

## ⚡ Performans Testleri

*Testler ~3.000 DAT dosyası (1 Milyon+ ROM girdisi) içeren bir veri seti üzerinde gerçekleştirilmiştir.*

| Strateji | Hız | RAM Kullanımı | Disk I/O |
| --- | --- | --- | --- |
| **In-Memory** | 🐢 Yavaş | 🔴 Yüksek | Düşük |
| **Staged** | 🐇 Hızlı | 🟢 Düşük | Yüksek (Geçici Dosyalar) |
| **Direct** | 🐆 **En Hızlı** | 🟢 Düşük | **Minimal** |

## 🔍 Örnek Sorgular (SQL)

Oluşturulan `.duckdb` dosyasını **DBeaver** veya **VSCode SQLTools** kullanarak açabilirsiniz.

**Doğrulanmış [!] Commodore 64 Oyunlarını Bul:**

```sql
SELECT game_name, rom_name 
FROM roms 
WHERE platform LIKE '%Commodore 64%' 
  AND rom_name LIKE '%[!]%';

```

**Mükerrer Kayıtları (Clone Checking) Bul:**

```sql
SELECT crc, COUNT(*) as count 
FROM roms 
GROUP BY crc 
HAVING count > 1 
ORDER BY count DESC;

```

## 📚 Dokümantasyon

Detaylı mimari açıklamaları ve ileri seviye kullanım senaryoları için lütfen **[Proje Wiki](https://github.com/deponeslabs/turbo-tosec/wiki)** sayfasına başvurun.

## 📄 Lisans

Bu proje **GNU General Public License v3.0 (GPL-3.0)** altında lisanslanmıştır.

---

## ❤️ Projeye Destek Olun

**turbo-tosec**, **Depones Labs** tarafından geliştirilmekte ve sürdürülmektedir. Eğer bu aracı yararlı bulduysanız, açık kaynak geliştirmeyi desteklemek için bağış yapmayı düşünebilirsiniz.

<a href="[https://github.com/sponsors/berkacunas](https://github.com/sponsors/berkacunas)">
<img src="[https://img.shields.io/badge/Sponsor-GitHub-pink?style=for-the-badge&logo=github-sponsors](https://img.shields.io/badge/Sponsor-GitHub-pink?style=for-the-badge&logo=github-sponsors)" height="50" alt="Sponsor on GitHub">
</a>

<a href="[https://www.buymeacoffee.com/depones](https://www.buymeacoffee.com/depones)" target="_blank">
<img src="[https://cdn.buymeacoffee.com/buttons/v2/default-yellow.png](https://cdn.buymeacoffee.com/buttons/v2/default-yellow.png)" alt="Buy Me A Coffee" style="height: 60px !important;width: 217px !important;">
</a>

---

*Yasal Uyarı: Bu proje TOSEC veritabanı dosyalarını veya ROM dosyalarını içermez. Sadece TOSEC projesi tarafından sağlanan metaveri dosyalarını işlemek için teknik bir araç sağlar.*

**Telif Hakkı © 2025 Depones Labs.**