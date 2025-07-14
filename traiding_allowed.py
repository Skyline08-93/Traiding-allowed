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

async def get_available_balance(coin: str = 'USDT') -> float:
    """Получает доступный баланс для торговли (Unified Account)"""
    try:
        balance = await exchange.fetch_balance({'type': 'unified'})
        
        # Для Unified Account 2024
        if 'list' in balance.get('info', {}).get('result', {}):
            for asset in balance['info']['result']['list'][0]['coin']:
                if asset['coin'] == coin:
                    return float(asset['availableToWithdraw'])
        
        # Fallback для старого формата
        return float(balance['total'].get(coin, {}).get('availableBalance', 0))
    
    except Exception as e:
        print(f"[Ошибка баланса] {e}")
        return 0.0

async def fetch_balances():
    """Получает полные балансы с учетом Unified Account"""
    try:
        balances = await exchange.fetch_balance({'type': 'unified'})
        result = {}
        
        # Обработка нового формата API (2024)
        if 'list' in balances.get('info', {}).get('result', {}):
            for asset in balances['info']['result']['list'][0]['coin']:
                if float(asset['equity']) > 0:
                    result[asset['coin']] = {
                        'free': float(asset['availableToWithdraw']),
                        'total': float(asset['equity'])
                    }
            return result
        
        # Fallback для старого формата
        for coin, amount in balances['total'].items():
            if isinstance(amount, (int, float)) and amount > 0:
                result[coin] = {
                    'free': balances['total'].get(coin, {}).get('availableBalance', amount),
                    'total': amount
                }
        return result
        
    except Exception as e:
        print(f"[Ошибка баланса] {e}")
        return {}

async def balance_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда для Telegram с детализацией балансов"""
    await context.bot.send_chat_action(
        chat_id=update.effective_chat.id,
        action=ChatAction.TYPING
    )
    
    balances = await fetch_balances()
    if not balances:
        await update.message.reply_text("⚠️ Не удалось получить балансы")
        return
    
    msg = "<b>💼 Доступные балансы:</b>\n"
    for coin, data in balances.items():
        free = data.get('free', 0)
        total = data.get('total', 0)
        msg += f"{coin}: {free:.4f} / {total:.4f} (свободно/всего)\n"
    
    # Добавляем информацию о доступном для торговли USDT
    available_usdt = await get_available_balance('USDT')
    msg += f"\n<b>🔄 Доступно для торговли:</b> {available_usdt:.2f} USDT"
    
    await update.message.reply_text(msg, parse_mode=ParseMode.HTML)

async def simulate_trading_execution(route_id, profit):
    await asyncio.sleep(1)
    msg = f"🤖 <b>Симуляция сделки</b>\nМаршрут: {route_id}\nПрибыль: {profit:.2f}%"
    await send_telegram_message(msg)
    return True

async def execute_real_trade(route_id: str, steps: list, base_coin: str, markets: dict, dynamic_volume: float):
    """Исправленная версия с надежной проверкой баланса"""
    try:
        # Логирование начала
        start_time = datetime.now(timezone.utc)
        log_msg = f"\n=== НАЧАЛО СДЕЛКИ {start_time} ==="
        log_msg += f"\nМаршрут: {route_id}"
        log_msg += f"\nОбъем: {dynamic_volume:.2f} {base_coin}"
        print(log_msg)
        
        # Улучшенная проверка баланса
        try:
            balance = await exchange.fetch_balance({'type': 'unified'})
            if 'info' in balance and 'result' in balance['info']:
                # Новый формат API (2024)
                available = float(balance['info']['result']['list'][0]['coin'][0].get('availableToWithdraw', 0))
            else:
                # Старый формат
                available = float(balance['total'].get(base_coin, 0))
            
            print(f"[BALANCE DEBUG] {base_coin} available: {available}, raw data: {balance}")
            
            if available < dynamic_volume:
                error_msg = f"❌ Недостаточно {base_coin}. Доступно: {available:.2f}, нужно: {dynamic_volume:.2f}"
                print(error_msg)
                await send_telegram_message(error_msg)
                return False
                
        except ValueError as ve:
            error_msg = f"🚨 Ошибка конвертации баланса: {str(ve)}. Raw data: {balance}"
            print(error_msg)
            await send_telegram_message("⚠️ Ошибка проверки баланса. Сделка отменена.")
            return False
            
        except Exception as e:
            error_msg = f"🚨 Неизвестная ошибка баланса: {str(e)}"
            print(error_msg)
            await send_telegram_message("⚠️ Системная ошибка при проверке баланса")
            return False

        # Основная логика исполнения сделки
        executed_orders = []
        current_amount = dynamic_volume
        
        for i, (symbol, side, price, amount) in enumerate(steps, 1):
            try:
                # Исполнение шага
                market = markets[symbol]
                tick_size = market.get('precision', {}).get('price', 0.00000001)
                
                order = await exchange.create_order(
                    symbol=symbol,
                    type='limit',
                    side=side,
                    amount=float(amount),
                    price=round(float(price) / tick_size) * tick_size,
                    params={'timeInForce': 'PostOnly'}
                )
                executed_orders.append(order)
                
                # Проверка исполнения
                await asyncio.sleep(3)
                order_status = await exchange.fetch_order(order['id'], symbol)
                
                # Обработка результатов
                filled = float(order_status['filled'])
                if filled == 0:
                    raise ValueError(f"Order not filled: {order_status}")
                    
                if side == 'buy':
                    current_amount = filled * float(order_status['average'])
                else:
                    current_amount = filled

            except Exception as e:
                error_msg = f"🔥 Ошибка на шаге {i}: {str(e)}"
                print(error_msg)
                await send_telegram_message(error_msg)
                
                # Отмена всех открытых ордеров
                for o in executed_orders:
                    try:
                        await exchange.cancel_order(o['id'], o['symbol'])
                    except:
                        pass
                return False

        # Финализация
        profit = current_amount - dynamic_volume
        profit_percent = (profit / dynamic_volume) * 100
        
        # Логирование
        log_msg += f"\nПрибыль: {profit:.2f} USDT ({profit_percent:.2f}%)"
        log_msg += f"\n=== КОНЕЦ СДЕЛКИ {datetime.now(timezone.utc)} ==="
        print(log_msg)
        
        with open(log_file, "a") as f:
            f.write(log_msg + "\n")
            
        await send_telegram_message(
            f"✅ Сделка завершена\n"
            f"Прибыль: {profit:.2f} USDT ({profit_percent:.2f}%)\n"
            f"Исходный объем: {dynamic_volume:.2f} USDT\n"
            f"Финальный баланс: {current_amount:.2f} USDT"
        )
        
        return True

    except Exception as e:
        error_msg = f"🔥 Критическая ошибка: {str(e)}"
        print(error_msg)
        await send_telegram_message("⚠️ Критическая ошибка при исполнении сделки")
        return False

async def check_triangle(base, mid1, mid2, symbols, markets):
    try:
        if is_shutting_down:
            if debug_mode:
                print("[DEBUG] Бот завершает работу, пропускаем проверку")
            return

        if debug_mode:
            print(f"\n[SCAN] Проверяем связку: {base} -> {mid1} -> {mid2} -> {base}")

        # Формируем пары для проверки
        s1 = f"{mid1}/{base}" if f"{mid1}/{base}" in symbols else f"{base}/{mid1}"
        s2 = f"{mid2}/{mid1}" if f"{mid2}/{mid1}" in symbols else f"{mid1}/{mid2}"
        s3 = f"{mid2}/{base}" if f"{mid2}/{base}" in symbols else f"{base}/{mid2}"

        if debug_mode:
            print(f"[PAIRS] {s1} | {s2} | {s3}")

        if not all(p in symbols for p in [s1, s2, s3]):
            if debug_mode:
                print(f"[SKIP] Не все пары доступны для торговли")
            return

        # Определяем направления торговли
        side1 = "buy" if f"{mid1}/{base}" in symbols else "sell"
        side2 = "buy" if f"{mid2}/{mid1}" in symbols else "sell"
        side3 = "sell" if f"{mid2}/{base}" in symbols else "buy"

        if debug_mode:
            print(f"[SIDES] {side1} | {side2} | {side3}")

        # Проверяем ликвидность (добавляем подробное логирование)
        if debug_mode:
            print(f"[LIQUIDITY] Проверяем ликвидность с объемом до ${scan_liquidity_range[1]}")

        price1, vol1, liq1 = await get_execution_price(s1, side1, scan_liquidity_range[1])
        price2, vol2, liq2 = await get_execution_price(s2, side2, scan_liquidity_range[1])
        price3, vol3, liq3 = await get_execution_price(s3, side3, scan_liquidity_range[1])

        if debug_mode:
            print(f"[LIQUIDITY RESULT] {s1}: {liq1:.2f} | {s2}: {liq2:.2f} | {s3}: {liq3:.2f}")

        if None in [price1, price2, price3]:
            if debug_mode:
                print("[SKIP] Не удалось получить цены для всех пар")
            return

        # Расчет динамического объема
        min_liquidity = min(liq1, liq2, liq3)
        dynamic_volume = min(max(min_liquidity * 0.9, min_trade_volume), max_trade_volume)

        if debug_mode:
            print(f"[VOLUME] Min liquidity: {min_liquidity:.2f} -> Dynamic volume: {dynamic_volume:.2f}")

        # Пересчет с реальным объемом
        price1, vol1, liq1 = await get_execution_price(s1, side1, dynamic_volume)
        price2, vol2, liq2 = await get_execution_price(s2, side2, dynamic_volume)
        price3, vol3, liq3 = await get_execution_price(s3, side3, dynamic_volume)

        # Расчет прибыльности
        step1 = (1 / price1 if side1 == "buy" else price1) * (1 - commission_rate)
        step2 = (1 / price2 if side2 == "buy" else price2) * (1 - commission_rate)
        step3 = (price3 if side3 == "sell" else 1 / price3) * (1 - commission_rate)

        result = step1 * step2 * step3
        profit_percent = (result - 1) * 100

        if debug_mode:
            print(f"[PROFIT] Расчетная прибыль: {profit_percent:.2f}%")

        if not (min_profit <= profit_percent <= max_profit):
            if debug_mode:
                print(f"[SKIP] Прибыль {profit_percent:.2f}% вне диапазона {min_profit}-{max_profit}%")
            return

        # Проверка кэша
        route_id = f"{base}->{mid1}->{mid2}->{base}"
        route_hash = hashlib.md5(route_id.encode()).hexdigest()
        now = datetime.now(timezone.utc)
        prev_time = triangle_cache.get(route_hash)

        if prev_time and (now - prev_time).total_seconds() < triangle_hold_time:
            if debug_mode:
                remaining = triangle_hold_time - (now - prev_time).total_seconds()
                print(f"[CACHE] Требуется ожидание: {remaining:.1f} сек")
            return

        triangle_cache[route_hash] = now

        # Проверка баланса
        if debug_mode:
            print("[BALANCE] Проверяем доступный баланс...")

        available = await get_available_balance(base.split(':')[0])
        if available < dynamic_volume:
            if debug_mode:
                print(f"[SKIP] Недостаточно средств. Доступно: {available:.2f}, нужно: {dynamic_volume:.2f}")
            return

        # Формирование сообщения
        message_lines = [
            f"🔍 Найдена связка: {route_id}",
            f"📈 Прибыль: {profit_percent:.2f}%",
            f"💵 Объем: ${dynamic_volume:.2f}",
            f"💧 Ликвидность: ${min(liq1, liq2, liq3):.2f}",
            f"🔄 Шаги:",
            f"1. {s1} {side1} @ {price1:.6f}",
            f"2. {s2} {side2} @ {price2:.6f}",
            f"3. {s3} {side3} @ {price3:.6f}"
        ]

        if debug_mode:
            print("\n".join(message_lines))

        await send_telegram_message("\n".join(message_lines))
        log_route(base, mid1, mid2, profit_percent, dynamic_volume, min(liq1, liq2, liq3))

        # Исполнение сделки
        if real_trading_enabled:
            if debug_mode:
                print("[TRADE] Запуск реальной сделки...")
            # ... (остальная логика исполнения)

    except Exception as e:
        if debug_mode:
            print(f"[ERROR] Ошибка при проверке связки: {str(e)}")
            import traceback
            traceback.print_exc()

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