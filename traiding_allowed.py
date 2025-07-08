import ccxt.async_support as ccxt
import asyncio
import os
import hashlib
import signal
from telegram.constants import ParseMode
from telegram.ext import Application, CommandHandler
from datetime import datetime, timezone

# === Telegram настройки ===
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
telegram_app = Application.builder().token(TELEGRAM_TOKEN).build()

# === Основные настройки ===
commission_rate = 0.001
min_profit = 0.1
max_profit = 3.0
target_volume_usdt = 100
real_trading_enabled = os.getenv("REAL_TRADING_ENABLED", "False") == "True"
debug_mode = True
triangle_cache = {}
triangle_hold_time = 5
log_file = "triangle_log.csv"
is_shutting_down = False

exchange = ccxt.bybit({
    "enableRateLimit": True,
    "apiKey": os.getenv("BYBIT_API_KEY"),
    "secret": os.getenv("BYBIT_API_SECRET")
})

def handle_signal(signum, frame):
    global is_shutting_down
    is_shutting_down = True
    print(f"Сигнал {signum} — завершение...")

signal.signal(signal.SIGINT, handle_signal)
signal.signal(signal.SIGTERM, handle_signal)

async def load_symbols():
    markets = await exchange.load_markets()
    return list(markets.keys()), markets

async def find_triangles(symbols, start_coins):
    triangles = []
    for base in start_coins:
        for sym1 in symbols:
            if not sym1.endswith('/' + base): continue
            mid1 = sym1.split('/')[0]
            for sym2 in symbols:
                if not sym2.startswith(mid1 + '/'): continue
                mid2 = sym2.split('/')[1]
                third = f"{mid2}/{base}"
                if third in symbols or f"{base}/{mid2}" in symbols:
                    triangles.append((base, mid1, mid2))
    return triangles

async def get_avg_price(orderbook_side, target_usdt):
    total_base = 0
    total_usd = 0
    max_liquidity = 0
    for price, volume in orderbook_side:
        price = float(price)
        volume = float(volume)
        usd = price * volume
        max_liquidity += usd
        if total_usd + usd >= target_usdt:
            remain_usd = target_usdt - total_usd
            total_base += remain_usd / price
            total_usd += remain_usd
            break
        else:
            total_base += volume
            total_usd += usd
    if total_usd < target_usdt:
        return None, 0, max_liquidity
    avg_price = total_usd / total_base
    return avg_price, total_usd, max_liquidity

async def get_execution_price(symbol, side, target_usdt):
    try:
        orderbook = await exchange.fetch_order_book(symbol)
        if side == "buy":
            return await get_avg_price(orderbook['asks'], target_usdt)
        else:
            return await get_avg_price(orderbook['bids'], target_usdt)
    except Exception as e:
        if debug_mode:
            print(f"[Ошибка стакана {symbol}]: {e}")
        return None, 0, 0

def format_line(index, pair, price, side, volume_usd, color, liquidity):
    emoji = {"green": "🟢", "yellow": "🟡", "red": "🟥"}.get(color, "")
    return f"{emoji} {index}. {pair} - {price:.6f} ({side}), исполнено ${volume_usd:.2f}, доступно ${liquidity:.2f}"

async def send_telegram_message(text):
    try:
        await telegram_app.bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=text, parse_mode=ParseMode.HTML)
    except Exception as e:
        if debug_mode:
            print(f"[Ошибка Telegram]: {e}")

def log_route(base, mid1, mid2, profit, volume):
    with open(log_file, "a") as f:
        f.write(f"{datetime.now(timezone.utc)},{base}->{mid1}->{mid2}->{base},{profit:.4f},{volume}\n")

async def fetch_balances():
    try:
        balances = await exchange.fetch_balance()
        return balances["total"]
    except Exception as e:
        if debug_mode:
            print(f"[Ошибка баланса]: {e}")
        return {}

async def simulate_trading_execution(route_id, profit):
    await asyncio.sleep(1)
    msg = f"🤖 <b>Симуляция сделки</b>\nМаршрут: {route_id}\nПрибыль: {profit:.2f}%"
    await send_telegram_message(msg)
    return True

async def check_triangle(base, mid1, mid2, symbols, markets):
    try:
        if is_shutting_down:
            return

        s1 = f"{mid1}/{base}" if f"{mid1}/{base}" in symbols else f"{base}/{mid1}"
        s2 = f"{mid2}/{mid1}" if f"{mid2}/{mid1}" in symbols else f"{mid1}/{mid2}"
        s3 = f"{mid2}/{base}" if f"{mid2}/{base}" in symbols else f"{base}/{mid2}"

        if not (s1 in symbols and s2 in symbols and s3 in symbols):
            return

        side1 = "buy" if f"{mid1}/{base}" in symbols else "sell"
        side2 = "buy" if f"{mid2}/{mid1}" in symbols else "sell"
        side3 = "sell" if f"{mid2}/{base}" in symbols else "buy"

        price1, vol1, liq1 = await get_execution_price(s1, side1, target_volume_usdt)
        if not price1: return
        step1 = (1 / price1 if side1 == "buy" else price1) * (1 - commission_rate)

        price2, vol2, liq2 = await get_execution_price(s2, side2, target_volume_usdt)
        if not price2: return
        step2 = (1 / price2 if side2 == "buy" else price2) * (1 - commission_rate)

        price3, vol3, liq3 = await get_execution_price(s3, side3, target_volume_usdt)
        if not price3: return
        step3 = (price3 if side3 == "sell" else 1 / price3) * (1 - commission_rate)

        result = step1 * step2 * step3
        profit_percent = (result - 1) * 100
        if not (min_profit <= profit_percent <= max_profit): return

        route_id = f"{base}->{mid1}->{mid2}->{base}"
        route_hash = hashlib.md5(route_id.encode()).hexdigest()
        now = datetime.now(timezone.utc)
        prev_time = triangle_cache.get(route_hash)
        execute = prev_time and (now - prev_time).total_seconds() >= triangle_hold_time
        if not execute:
            triangle_cache[route_hash] = now

        min_liquidity = round(min(liq1, liq2, liq3), 2)
        pure_profit_usdt = round((result - 1) * target_volume_usdt, 2)

        message = "\n".join([
            format_line(1, s1, price1, side1.upper(), vol1, "green", liq1),
            format_line(2, s2, price2, side2.upper(), vol2, "yellow", liq2),
            format_line(3, s3, price3, side3.upper(), vol3, "red", liq3),
            "",
            f"💰 Чистая прибыль: {pure_profit_usdt:.2f} USDT",
            f"📈 Спред: {profit_percent:.2f}%",
            f"💧 Мин. ликвидность на шаге: ${min_liquidity}",
            f"⚙️ Готов к сделке: {'ДА' if execute else 'НЕТ'}",
            f"🔧 Режим: {'РЕАЛЬНАЯ ТОРГОВЛЯ' if real_trading_enabled else 'СИМУЛЯЦИЯ'}"
        ])

        if debug_mode:
            print(message)

        await send_telegram_message(message)
        log_route(base, mid1, mid2, profit_percent, min_liquidity)

        if execute:
            balances = await fetch_balances()
            free_amount = balances.get(base, 0)
            if free_amount < target_volume_usdt:
                msg = f"❌ Недостаточно {base} на балансе. Доступно: {free_amount:.2f} {base}, нужно: {target_volume_usdt:.2f} {base}"
                print(msg)
                await send_telegram_message(msg)
                return
            await simulate_trading_execution(route_id, profit_percent)

    except Exception as e:
        if debug_mode:
            print(f"[Ошибка маршрута {base}->{mid1}->{mid2}]: {e}")

# === /balance команда
async def balance_command(update, context):
    balances = await fetch_balances()
    if not balances:
        await update.message.reply_text("⚠️ Не удалось получить балансы.")
        return
    msg = "♻️ <b>Балансы на Bybit:</b>\n"
    for coin, amount in balances.items():
        if amount > 0:
            msg += f"{coin}: {amount:.6f}\n"
    await update.message.reply_text(msg, parse_mode=ParseMode.HTML)

async def main():
    try:
        print("Запуск треугольного арбитражного бота")
        print(f"Режим: {'РЕАЛЬНАЯ ТОРГОВЛЯ' if real_trading_enabled else 'СИМУЛЯЦИЯ'}")
        print(f"Целевой объем: {target_volume_usdt} USDT")

        symbols, markets = await load_symbols()
        start_coins = list(set([s.split('/')[1] for s in symbols if '/' in s]))
        triangles = await find_triangles(symbols, start_coins)
        print(f"🔁 Найдено треугольников: {len(triangles)}")

        telegram_app.add_handler(CommandHandler("balance", balance_command))
        await telegram_app.initialize()
        await telegram_app.start()
        await send_telegram_message("🚀 Бот запущен!")

        while not is_shutting_down:
            tasks = [check_triangle(base, mid1, mid2, symbols, markets) for base, mid1, mid2 in triangles]
            await asyncio.gather(*tasks)
            await asyncio.sleep(10)

    except Exception as e:
        print(f"[КРИТИЧЕСКАЯ ОШИБКА]: {e}")
        await send_telegram_message(f"🔥 Ошибка: {str(e)}")
    finally:
        print("Завершение...")
        await telegram_app.stop()
        await telegram_app.shutdown()
        await exchange.close()

if __name__ == '__main__':
    asyncio.run(main())
