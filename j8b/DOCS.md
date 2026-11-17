# j8b

Connects the Home Assistant in your home to the **j8b** app (web + mobile). Your home opens no port to the outside; the add-on itself establishes a secure connection to the cloud.

## Setup (English)
1. Install the add-on and **Start** it (it waits for pairing on first run).
2. In the j8b app choose **Add home** → get the 8-character code (valid 15 minutes).
3. In this add-on's **Configuration** tab enter the code in `pair_code`, **Save**, then **Restart** the add-on.
4. Within seconds your home shows as "online" in the app. Then **Choose devices**: only the state of the devices you tick goes to the cloud.
5. Turn on the **Watchdog** switch on the add-on page so Home Assistant restarts it automatically if it ever stops (important on memory-constrained devices such as a Raspberry Pi 3).

## Privacy
Only the current state of the devices you **expose**, your home's online status and the commands you send reach the cloud. Sensor history, cameras, automations and credentials stay at home. Details: https://j8b.net/en/privacy

---

Evindeki Home Assistant'ı **j8b** uygulamasına (web + mobil) bağlar. Ev dışarıya hiçbir port açmaz; eklenti buluta kendisi güvenli bağlantı kurar.

## Kurulum (Türkçe)
1. Eklentiyi kur ve **Başlat** (ilk açılışta eşleşme bekler).
2. j8b uygulamasında **Ev ekle** → 8 haneli kodu al (15 dk geçerli).
3. Bu eklentinin **Yapılandırma** sekmesinde `pair_code` alanına kodu yaz, **Kaydet**, eklentiyi **Yeniden başlat**.
4. Ev birkaç saniye içinde uygulamada "çevrimiçi" görünür. Sonra **Cihazları seç**: yalnız işaretlediklerinin durumu buluta gider.
5. Eklenti sayfasında **Gözetim (Watchdog)** anahtarını açın: eklenti herhangi bir sebeple durursa Home Assistant onu kendiliğinden yeniden başlatır. Belleği kısıtlı cihazlarda (Raspberry Pi 3 gibi) bu önemlidir.

## Gizlilik
Buluta yalnız **açığa çıkardığın** cihazların anlık durumu, evin çevrimiçi bilgisi ve verdiğin komutlar gider. Sensör geçmişi, kamera, otomasyonlar, kimlik bilgileri evde kalır. Ayrıntı: https://j8b.net/guvenlik

## Seçenekler
- `pair_code` — yalnız ilk eşleşmede; sonrası `/data/home.json`'da saklanır (silinirse yeni kod gerekir).
- `api_url` — varsayılan `https://api.j8b.net`.
- `log_level` — `info` (varsayılan) / `debug` / `warning`.
