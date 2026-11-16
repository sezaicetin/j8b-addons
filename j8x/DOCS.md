# j8x

Evindeki Home Assistant'ı **j8x** uygulamasına (web + mobil) bağlar. Ev dışarıya hiçbir port açmaz; eklenti buluta kendisi güvenli bağlantı kurar.

## Kurulum
1. Eklentiyi kur ve **Başlat** (ilk açılışta eşleşme bekler).
2. j8x uygulamasında **Ev ekle** → 8 haneli kodu al (15 dk geçerli).
3. Bu eklentinin **Yapılandırma** sekmesinde `pair_code` alanına kodu yaz, **Kaydet**, eklentiyi **Yeniden başlat**.
4. Ev birkaç saniye içinde uygulamada "çevrimiçi" görünür. Sonra **Cihazları seç**: yalnız işaretlediklerinin durumu buluta gider.
5. Eklenti sayfasında **Gözetim (Watchdog)** anahtarını açın: eklenti herhangi bir sebeple durursa Home Assistant onu kendiliğinden yeniden başlatır. Belleği kısıtlı cihazlarda (Raspberry Pi 3 gibi) bu önemlidir.

## Gizlilik
Buluta yalnız **açığa çıkardığın** cihazların anlık durumu, evin çevrimiçi bilgisi ve verdiğin komutlar gider. Sensör geçmişi, kamera, otomasyonlar, kimlik bilgileri evde kalır. Ayrıntı: https://j8b.net/guvenlik

## Seçenekler
- `pair_code` — yalnız ilk eşleşmede; sonrası `/data/home.json`'da saklanır (silinirse yeni kod gerekir).
- `api_url` — varsayılan `https://api.j8b.net`.
- `log_level` — `info` (varsayılan) / `debug` / `warning`.
