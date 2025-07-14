# === traiding_allowed.py (полная версия с логированием) ===
import ccxt.async_support as ccxt
import asyncio
import os
import hashlib
import signal
import time
from telegram import Update
from telegram.constants import ParseMode, ChatAction
from telegram.ext import Application, CommandHandler, ContextTypes
from datetime import datetime, timezone

def clean_symbol(symbol):
    return symbol.replace(":USDT", "").replace(":BTC", "").replace(":ETH", "").replace(":", "")

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
telegram_app = Application.builder().token(TELEGRAM_TOKEN).build()

# Настройки
commission_rate = 0.001
min_profit = 0.1
max_profit = 3.0
min_trade_volume = 10    # Минимальный объем сделки в USDT
max_trade_volume = 100   # Максимальный объем сделки в USDT
scan_liquidity_range = (10, 1000)  # Диапазон ликвидности для сканирования
real_trading_enabled = os.getenv("REAL_TRADING_ENABLED", "False") == "True"
debug_mode = True
triangle_cache = {}
triangle_hold_time = 5
log_file = "triangle_log.csv"
is_shutting_down = False

# Кеш стаканов
orderbook_cache = {}
orderbook_cache_ttl = 5  # секунд

exchange = ccxt.bybit({
    "options": {
        "defaultType": "unified"
    },
    "enableRateLimit": True,
    "apiKey": os.getenv("BYBIT_API_KEY"),
    "secret": os.getenv("BYBIT_API_SECRET")
})

def handle_signal(signum, frame):
    global is_shutting_down
    is_shutting_down = True

signal.signal(signal.SIGINT, handle_signal)
signal.signal(signal.SIGTERM, handle_signal)

async def get_cached_orderbook(symbol):
    now = time.time()
    if symbol in orderbook_cache:
        cached_time, orderbook = orderbook_cache[symbol]
        if now - cached_time < orderbook_cache_ttl:
            return orderbook
    
    orderbook = await exchange.fetch_order_book(symbol)
    orderbook_cache[symbol] = (now, orderbook)
    return orderbook

async def load_symbols():
    markets = await exchange.load_markets()
    return [s for s in markets.keys() if ":" not in s], markets

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
        orderbook = await get_cached_orderbook(symbol)
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

def log_route(base, mid1, mid2, profit, volume, min_liquidity):
    with open(log_file, "a") as f:
        f.write(f"{datetime.now(timezone.utc)},{base}->{mid1}->{mid2}->{base},{profit:.4f},{volume:.2f},{min_liquidity:.2f}\n")

async def fetch_balances():
    try:
        balances = await exchange.fetch_balance()
        result = {}
        for coin, entry in balances.get("total", {}).items():
            if isinstance(entry, (int, float)) and entry > 0:
                result[coin] = entry
            elif isinstance(entry, dict):
                free = entry.get("free")
                total = entry.get("total", 0)
                if (free is not None and free > 0):
                    result[coin] = free
                elif total > 0:
                    result[coin] = total
        return result
    except Exception as e:
        if debug_mode:
            print(f"[Ошибка баланса]: {e}")
        return {}

async def balance_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await context.bot.send_chat_action(
        chat_id=update.effective_chat.id,
        action=ChatAction.TYPING
    )
    
    balances = await fetch_balances()
    if not balances:
        await update.message.reply_text("⚠️ Не удалось получить балансы.")
        return
        
    msg = "<b>💼 Балансы:</b>\n"
    for coin, amount in balances.items():
        if amount > 0:
            msg += f"{coin}: {amount:.4f}\n"
            
    await update.message.reply_text(msg, parse_mode=ParseMode.HTML)

async def simulate_trading_execution(route_id, profit):
    await asyncio.sleep(1)
    msg = f"🤖 <b>Симуляция сделки</b>\nМаршрут: {route_id}\nПрибыль: {profit:.2f}%"
    await send_telegram_message(msg)
    return True

async def execute_real_trade(route_id, steps, base_coin, markets):
    try:
        await send_telegram_message(f"🚀 Запуск торговли по маршруту: {route_id} | Объем: ${dynamic_volume:.2f}")

        balance = await exchange.fetch_balance()
        asset_info = balance.get(base_coin, {})
        free_balance = asset_info.get("free")
        if free_balance is None:
            await send_telegram_message("⚠️ Использую total вместо free — вероятно, Unified аккаунт Bybit")
            free_balance = asset_info.get("total", 0)

        actual_volume = min(free_balance, dynamic_volume) * 0.98

        if actual_volume < min_trade_volume:
            await send_telegram_message(
                f"❌ Недостаточно {base_coin} на балансе. Доступно: {free_balance:.2f}, нужно минимум: {min_trade_volume:.2f}"
            )
            return False

        await send_telegram_message(f"📊 Итоговый объем для торговли: {actual_volume:.4f} {base_coin}")

        executed_orders = []
        current_amount = actual_volume

        for i, (symbol, side, price, _) in enumerate(steps, 1):
            try:
                market = markets[symbol]
                tick_size = market.get("precision", {}).get("price")
                min_price = market.get("limits", {}).get("price", {}).get("min")
                min_amount = market.get("limits", {}).get("amount", {}).get("min", 0)

                if min_price is None:
                    await send_telegram_message(f"⚠️ {symbol}: min_price отсутствует, fallback → 0.0")
                    min_price = 0.0

                if price is None or price < min_price:
                    await send_telegram_message(f"❌ Шаг {i}: цена {price} некорректна для {symbol}")
                    return False

                if tick_size:
                    price = round(price / tick_size) * tick_size

                # Расчёт объёма на этом шаге
                amount = current_amount / price if side == "buy" else current_amount
                amount = max(amount, min_amount)
                adjusted_amount = amount * 0.98

                await send_telegram_message(f"📦 Шаг {i}: {symbol} {side.upper()} @ {price} | Объем: {adjusted_amount:.6f}")

                order = await exchange.create_order(
                    symbol=symbol,
                    type="limit",
                    side=side,
                    amount=adjusted_amount,
                    price=price,
                    params={}
                )
                executed_orders.append(order)
                await asyncio.sleep(3)

                matched_order = None
                closed_orders = await exchange.fetch_closed_orders(symbol)
                matched_order = next((o for o in closed_orders if o["id"] == order["id"]), None)

                if matched_order is None:
                    open_orders = await exchange.fetch_open_orders(symbol)
                    matched_order = next((o for o in open_orders if o["id"] == order["id"]), None)
                    if matched_order is not None:
                        await send_telegram_message(f"⚠️ Шаг {i}: Ордер {symbol} ещё открыт. Возможно, не исполнен.")
                        await exchange.cancel_order(order["id"], symbol)
                        return False

                if matched_order is None:
                    await send_telegram_message(f"❌ Шаг {i}: Ордер {symbol} не найден среди closed/open")
                    return False

                filled = float(matched_order.get("filled", 0))
                avg_price = float(matched_order.get("average") or price)

                if filled == 0:
                    await exchange.cancel_order(order["id"], symbol)
                    await send_telegram_message(f"❌ Шаг {i}: Ордер исполнен на 0 — {symbol}")
                    return False

                if side == "buy":
                    current_amount = filled * avg_price * (1 - commission_rate)
                else:
                    current_amount = filled * (1 - commission_rate)

                await send_telegram_message(f"✅ Шаг {i}: исполнено {filled:.6f} @ {avg_price:.6f}")

            except Exception as e:
                await send_telegram_message(f"🔥 Ошибка на шаге {i}: {e}")
                for o in executed_orders:
                    try:
                        await exchange.cancel_order(o["id"], o["symbol"])
                    except:
                        pass
                return False

        profit_usdt = current_amount - actual_volume
        profit_percent = (profit_usdt / actual_volume) * 100

        summary = f"""✅ <b>РЕАЛЬНАЯ СДЕЛКА</b>
Маршрут: {route_id}
Начальный объем: ${actual_volume:.2f}
Финальный объем: ${current_amount:.2f}
💵 Прибыль: ${profit_usdt:.2f} ({profit_percent:.2f}%)"""

        await send_telegram_message(summary)
        return True

    except Exception as e:
        await send_telegram_message(f"🔥 Критическая ошибка торговли: {e}")
        return False

async def check_triangle(base, mid1, mid2, symbols, markets):
    try:
        if is_shutting_down:
            return

        s1 = f"{mid1}/{base}" if f"{mid1}/{base}" in symbols else f"{base}/{mid1}"
        s2 = f"{mid2}/{mid1}" if f"{mid2}/{mid1}" in symbols else f"{mid1}/{mid2}"
        s3 = f"{mid2}/{base}" if f"{mid2}/{base}" in symbols else f"{base}/{mid2}"

        if not (s1 in symbols and s2 in symbols and s3 in symbols):
            return

        s1_clean = clean_symbol(s1)
        s2_clean = clean_symbol(s2)
        s3_clean = clean_symbol(s3)

        side1 = "buy" if f"{mid1}/{base}" in symbols else "sell"
        side2 = "buy" if f"{mid2}/{mid1}" in symbols else "sell"
        side3 = "sell" if f"{mid2}/{base}" in symbols else "buy"

        price1, vol1, liq1 = await get_execution_price(s1, side1, 100)
        if not price1:
            return
        step1 = (1 / price1 if side1 == "buy" else price1) * (1 - commission_rate)

        price2, vol2, liq2 = await get_execution_price(s2, side2, 100)
        if not price2:
            return
        step2 = (1 / price2 if side2 == "buy" else price2) * (1 - commission_rate)

        price3, vol3, liq3 = await get_execution_price(s3, side3, 100)
        if not price3:
            return
        step3 = (price3 if side3 == "sell" else 1 / price3) * (1 - commission_rate)

        result = step1 * step2 * step3
        profit_percent = (result - 1) * 100

        if profit_percent < min_profit or profit_percent > max_profit:
            return

        min_liquidity = min(liq1, liq2, liq3)
        if min_liquidity < 10 or min_liquidity > 1000:
            return

        # 🔧 Гибкий торговый объём от 10 до 100 USDT
        min_trade_volume = 10
        max_trade_volume = 100
        dynamic_volume = min(
            max(min_liquidity * 0.9, min_trade_volume),
            max_trade_volume
        )

        route_id = f"{base}->{mid1}->{mid2}->{base}"
        route_hash = hashlib.md5(route_id.encode()).hexdigest()
        now = datetime.now(timezone.utc)
        prev_time = triangle_cache.get(route_hash)
        execute = prev_time and (now - prev_time).total_seconds() >= triangle_hold_time

        if not execute:
            triangle_cache[route_hash] = now
            return

        amount1 = dynamic_volume
        amount2 = amount1 / price1 if side1 == 'buy' else amount1 * price1
        amount3 = amount2 / price2 if side2 == 'buy' else amount2 * price2

        trade_steps = [
            (s1, side1, price1, amount1),
            (s2, side2, price2, amount2),
            (s3, side3, price3, amount3)
        ]

        await send_telegram_message("\n".join([
            format_line(1, s1_clean, price1, side1.upper(), vol1, "green", liq1),
            format_line(2, s2_clean, price2, side2.upper(), vol2, "yellow", liq2),
            format_line(3, s3_clean, price3, side3.upper(), vol3, "red", liq3),
            "",
            f"💰 Чистая прибыль: {(result - 1) * dynamic_volume:.2f} USDT",
            f"📈 Спред: {profit_percent:.2f}%",
            f"💧 Мин. ликвидность на шаге: ${min_liquidity:.2f}",
            f"⚙️ Готов к сделке: {'ДА' if execute else 'НЕТ'}",
            f"🔧 Режим: {'РЕАЛЬНАЯ ТОРГОВЛЯ' if real_trading_enabled else 'СИМУЛЯЦИЯ'}"
        ]))

        if real_trading_enabled:
            await execute_real_trade(route_id, trade_steps, base, markets, dynamic_volume)
        else:
            await simulate_trading_execution(route_id, profit_percent)

    except Exception as e:
        if debug_mode:
            print(f"[Ошибка маршрута {base}->{mid1}->{mid2}]: {e}")

async def main():
    symbols, markets = await load_symbols()
    start_coins = ['USDT']
    triangles = await find_triangles(symbols, start_coins)
    print(f"🔁 Найдено треугольников: {len(triangles)}")

    telegram_app.add_handler(CommandHandler("balance", balance_command))
    await telegram_app.initialize()
    await telegram_app.start()
    await send_telegram_message("♻️ Бот запущен!")

    while not is_shutting_down:
        tasks = [check_triangle(base, mid1, mid2, symbols, markets) for base, mid1, mid2 in triangles]
        await asyncio.gather(*tasks)
        await asyncio.sleep(10)

    await telegram_app.stop()
    await telegram_app.shutdown()
    await exchange.close()

if __name__ == '__main__':
    asyncio.run(main())