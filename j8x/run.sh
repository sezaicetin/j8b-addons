#!/usr/bin/with-contenv bashio
# j8b add-on giris noktasi: options -> ortam; ajan /data/home.json'da esleseme saklar.
export LOG_LEVEL="$(bashio::config 'log_level' | tr '[:lower:]' '[:upper:]')"
if ! bashio::config.has_value 'pair_code' && [ ! -f /data/home.json ]; then
  bashio::log.warning "Eslestirme kodu girilmedi: Yapilandirma sekmesine j8b uygulamasindaki 8 haneli kodu yazin, kaydedin, eklentiyi yeniden baslatin."
fi
exec python3 /app/j8b_agent.py
