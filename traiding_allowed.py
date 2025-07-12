# === traiding_allowed.py (финальный) ===
import ccxt.async_support as ccxt
import asyncio
import os
import hashlib
import signal
from telegram import Update
from telegram.constants import ParseMode, ChatAction
from telegram.ext import Application, CommandHandler, ContextTypes
from datetime import datetime, timezone

def clean_symbol(symbol):
    return symbol.replace(":USDT", "").replace(":BTC", "").replace(":ETH", "").replace(":", "")

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
telegram_app = Application.builder().token(TELEGRAM_TOKEN).build()

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
        result = {}
        for coin, entry in balances.get("total", {}).items():
            if isinstance(entry, (int, float)) and entry > 0:
                result[coin] = entry
            elif isinstance(entry, dict):
                # Обрабатываем {'free': None, 'total': 100.0}
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
    # Уведомляем пользователя, что бот "печатает"
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
        await send_telegram_message(f"🚀 Запуск торговли по маршруту: {route_id}")
        print(f"[ТОРГОВЛЯ] Маршрут: {route_id}")

        balance = await exchange.fetch_balance()
        asset_info = balance.get(base_coin, {})
        print(f"[ОТЛАДКА] Баланс {base_coin}: {asset_info}")

        try:
            await send_telegram_message(f"📊 Баланс {base_coin} (debug):\n{str(asset_info)[:1000]}")
        except Exception as e:
            print(f"[ОШИБКА Telegram]: {e}")

        free_balance = asset_info.get("free")
        if free_balance is None:
            print("[⚠️] Баланс 'free' отсутствует, fallback на 'total'")
            await send_telegram_message("⚠️ Использую total вместо free — вероятно, Unified аккаунт Bybit")
            free_balance = asset_info.get("total", 0)

        print(f"[ТОРГОВЛЯ] Баланс {base_coin}: {free_balance:.2f}")

        if free_balance < target_volume_usdt:
            msg = f"❌ Недостаточно {base_coin}. Доступно: {free_balance:.2f}, нужно: {target_volume_usdt:.2f}"
            await send_telegram_message(msg)
            print("[ТОРГОВЛЯ ОТКЛОНЕНА] Недостаточно средств")
            return False

        executed_orders = []
        current_amount = target_volume_usdt

        for i, (symbol, side, price, amount) in enumerate(steps, 1):
            try:
                market = markets[symbol]
                tick_size = market.get("precision", {}).get("price")
                min_price = market.get("limits", {}).get("price", {}).get("min")
                min_amount = market.get("limits", {}).get("amount", {}).get("min", 0)

                print(f"[ШАГ {i}] {symbol} {side} @ {price} | amount: {amount}")
                await send_telegram_message(f"📦 Шаг {i}: {symbol} {side.upper()} @ {price} | Объем: {amount:.6f}")

                # 🔧 Защита от отсутствия min_price
                if min_price is None:
                    print(f"[⚠️] {symbol}: min_price отсутствует — устанавливаем в 0.0")
                    await send_telegram_message(f"⚠️ {symbol}: min_price отсутствует, fallback → 0.0")
                    min_price = 0.0

                if price is None:
                    msg = f"❌ Шаг {i}: отсутствует цена для {symbol}"
                    await send_telegram_message(msg)
                    print(f"[ОШИБКА] {msg}")
                    return False

                if price < min_price:
                    msg = f"❌ Шаг {i}: цена {price} ниже минимальной {min_price} для {symbol}"
                    await send_telegram_message(msg)
                    print(f"[ОШИБКА] {msg}")
                    return False

                if tick_size:
                    price = round(price / tick_size) * tick_size

                amount = max(amount, min_amount)
                adjusted_amount = amount * (1 - commission_rate)

                order = await exchange.create_order(
                    symbol=symbol,
                    type="limit",
                    side=side,
                    amount=adjusted_amount,
                    price=price,
                    params={"timeInForce": "PostOnly"},
                )
                executed_orders.append(order)
                await asyncio.sleep(1.5)

                status = await exchange.fetch_order(order["id"], symbol)
                filled = float(status.get("filled", 0))
                avg_price = float(status.get("average") or price)

                if filled == 0:
                    await exchange.cancel_order(order["id"], symbol)
                    await send_telegram_message(f"❌ Шаг {i}: Ордер не исполнен — {symbol} {side}")
                    return False

                if side == "buy":
                    current_amount = filled * avg_price * (1 - commission_rate)
                else:
                    current_amount = filled * (1 - commission_rate)

                await send_telegram_message(f"✅ Шаг {i}: исполнено {filled:.6f} @ {avg_price:.6f}")
                print(f"[ИСПОЛНЕНО] {symbol} {side}: {filled} @ {avg_price}")

            except Exception as e:
                await send_telegram_message(f"🔥 Ошибка на шаге {i}: {e}")
                print(f"[ОШИБКА] шаг {i}: {e}")
                for o in executed_orders:
                    try:
                        await exchange.cancel_order(o["id"], o["symbol"])
                    except:
                        pass
                return False

        profit_usdt = current_amount - target_volume_usdt
        profit_percent = (profit_usdt / target_volume_usdt) * 100

        summary = f"""✅ <b>РЕАЛЬНАЯ СДЕЛКА</b>
Маршрут: {route_id}
Начальный объем: ${target_volume_usdt:.2f}
Финальный объем: ${current_amount:.2f}
💵 Прибыль: ${profit_usdt:.2f} ({profit_percent:.2f}%)"""

        await send_telegram_message(summary)
        print(summary)
        return True

    except Exception as e:
        await send_telegram_message(f"🔥 Критическая ошибка торговли: {e}")
        print(f"[КРИТИЧЕСКАЯ ОШИБКА]: {e}")
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

        price1, vol1, liq1 = await get_execution_price(s1, side1, target_volume_usdt)
        if not price1:
            if debug_mode:
                print(f"[НЕТ] {base}->{mid1}->{mid2}: Не удалось получить цену для {s1_clean}")
            return
        step1 = (1 / price1 if side1 == "buy" else price1) * (1 - commission_rate)

        price2, vol2, liq2 = await get_execution_price(s2, side2, target_volume_usdt)
        if not price2:
            if debug_mode:
                print(f"[НЕТ] {base}->{mid1}->{mid2}: Не удалось получить цену для {s2_clean}")
            return
        step2 = (1 / price2 if side2 == "buy" else price2) * (1 - commission_rate)

        price3, vol3, liq3 = await get_execution_price(s3, side3, target_volume_usdt)
        if not price3:
            if debug_mode:
                print(f"[НЕТ] {base}->{mid1}->{mid2}: Не удалось получить цену для {s3_clean}")
            return
        step3 = (price3 if side3 == "sell" else 1 / price3) * (1 - commission_rate)

        result = step1 * step2 * step3
        profit_percent = (result - 1) * 100
        
        if profit_percent < min_profit or profit_percent > max_profit:
            if debug_mode:
                print(f"[НЕТ] {base}->{mid1}->{mid2}: Прибыль {profit_percent:.2f}% вне диапазона")
            return

        route_id = f"{base}->{mid1}->{mid2}->{base}"
        route_hash = hashlib.md5(route_id.encode()).hexdigest()
        now = datetime.now(timezone.utc)
        prev_time = triangle_cache.get(route_hash)
        execute = prev_time and (now - prev_time).total_seconds() >= triangle_hold_time
        
        if not execute:
            triangle_cache[route_hash] = now
            if debug_mode:
                remaining = triangle_hold_time - (now - prev_time).total_seconds() if prev_time else triangle_hold_time
                print(f"[НЕТ] {route_id}: Требуется ожидание {remaining:.1f} сек")
            return

        min_liquidity = round(min(liq1, liq2, liq3), 2)
        pure_profit_usdt = round((result - 1) * target_volume_usdt, 2)

        balances = await fetch_balances()
        asset = base.split(":")[0].replace(":", "")
        free_amount = balances.get(asset, 0)
        
        if free_amount < target_volume_usdt:
            msg = f"❌ Недостаточно {asset} на балансе. Доступно: {free_amount:.2f} {asset}, нужно: {target_volume_usdt:.2f}"
            if debug_mode:
                print(f"[НЕТ] {route_id}: {msg}")
            await send_telegram_message(msg)
            return

        ready_text = "ДА" if execute else "НЕТ"
        message = "\n".join([
            format_line(1, s1_clean, price1, side1.upper(), vol1, "green", liq1),
            format_line(2, s2_clean, price2, side2.upper(), vol2, "yellow", liq2),
            format_line(3, s3_clean, price3, side3.upper(), vol3, "red", liq3),
            "",
            f"💰 Чистая прибыль: {pure_profit_usdt:.2f} USDT",
            f"📈 Спред: {profit_percent:.2f}%",
            f"💧 Мин. ликвидность на шаге: ${min_liquidity}",
            f"⚙️ Готов к сделке: {ready_text}",
            f"🔧 Режим: {'РЕАЛЬНАЯ ТОРГОВЛЯ' if real_trading_enabled else 'СИМУЛЯЦИЯ'}"
        ])

        if debug_mode:
            print(message)

        await send_telegram_message(message)
        log_route(base, mid1, mid2, profit_percent, min_liquidity)

        if execute:
            print(f"[ТРЕУГОЛЬНИК] {route_id}: ПРОВЕРЕН — ГОТОВ К СДЕЛКЕ")
        await send_telegram_message(f"🔍 Треугольник найден и готов к сделке:\n{route_id}")
        if real_trading_enabled:
                amount1 = target_volume_usdt
                amount2 = amount1 / price1 if side1 == 'buy' else amount1 * price1
                amount3 = amount2 / price2 if side2 == 'buy' else amount2 * price2

                trade_steps = [
                    (s1, side1, price1, amount1),
                    (s2, side2, price2, amount2),
                    (s3, side3, price3, amount3)
                ]
                await execute_real_trade(route_id, trade_steps, asset, markets)
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
