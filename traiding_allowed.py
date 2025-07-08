import ccxt.async_support as ccxt
import asyncio
import os
import hashlib
import signal
from telegram import Bot
from telegram.constants import ParseMode
from telegram.ext import Application
from datetime import datetime, timedelta

# === Инициализация ===
is_shutting_down = False

# === Telegram настройки ===
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
telegram_app = Application.builder().token(TELEGRAM_TOKEN).build()

# === Основные настройки ===
commission_rate = 0.001
min_profit = float(os.getenv("MIN_PROFIT", "0.1"))
max_profit = float(os.getenv("MAX_PROFIT", "3.0"))
start_coins = ['USDT', 'BTC', 'ETH']
real_trading_enabled = os.getenv("REAL_TRADING_ENABLED", "True") == "True"
target_volume_usdt = float(os.getenv("TARGET_VOLUME_USDT", "10"))
debug_mode = True
log_file = "triangle_log.csv"
triangle_cache = {}
triangle_hold_time = 5

exchange = ccxt.bybit({
    "enableRateLimit": True,
    "apiKey": os.getenv("BYBIT_API_KEY"),
    "secret": os.getenv("BYBIT_API_SECRET")
})

# Обработчик сигналов для graceful shutdown
def handle_signal(signum, frame):
    global is_shutting_down
    is_shutting_down = True
    print(f"Получен сигнал {signum}, завершаем работу...")

signal.signal(signal.SIGINT, handle_signal)
signal.signal(signal.SIGTERM, handle_signal)

async def load_symbols():
    markets = await exchange.load_markets()
    return list(markets.keys()), markets

async def find_triangles(symbols):
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
    
    # Сниженные требования к ликвидности (50% от целевого объема)
    if total_usd < target_usdt * 0.5:
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
        f.write(f"{datetime.utcnow()},{base}->{mid1}->{mid2}->{base},{profit:.4f},{volume}\n")

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

async def execute_real_trade(route_id, steps, base_coin, markets):
    try:
        # Проверка флага завершения
        if is_shutting_down:
            print("Пропуск сделки: идет завершение работы")
            return False
            
        balance = await exchange.fetch_balance()
        if balance[base_coin]['free'] < target_volume_usdt:
            await send_telegram_message(f"❌ Недостаточно {base_coin} для сделки. Доступно: {balance[base_coin]['free']:.2f}")
            return False

        executed_orders = []
        current_amount = target_volume_usdt
        
        for i, (symbol, side, price, amount) in enumerate(steps, 1):
            try:
                # Получаем параметры рынка
                market = markets[symbol]
                tick_size = float(market['precision']['price'])
                min_price = float(market['limits']['price']['min'])
                min_amount = float(market['limits']['amount']['min'])
                
                # Округление до допустимых значений
                price = round(price / tick_size) * tick_size
                amount = max(amount, min_amount)
                
                # Проверка минимальных значений
                if price < min_price:
                    await send_telegram_message(f"❌ Цена {price} ниже минимальной {min_price} для {symbol}")
                    return False
                    
                if amount < min_amount:
                    await send_telegram_message(f"❌ Объем {amount} меньше минимального {min_amount} для {symbol}")
                    return False
                    
                adjusted_amount = amount * (1 - commission_rate)
                
                order = await exchange.create_order(
                    symbol=symbol,
                    type='limit',
                    side=side,
                    amount=adjusted_amount,
                    price=price,
                    params={'timeInForce': 'PostOnly'}
                )
                executed_orders.append(order)
                
                await asyncio.sleep(1)
                order_status = await exchange.fetch_order(order['id'], symbol)
                
                if order_status['status'] != 'closed':
                    await exchange.cancel_order(order['id'], symbol)
                    await send_telegram_message(f"❌ Ордер {i} не исполнен: {symbol} {side} {amount}@{price}")
                    return False
                
                if side == 'buy':
                    current_amount = order_status['filled'] * order_status['price']
                else:
                    current_amount = order_status['filled']
                    
            except Exception as e:
                await send_telegram_message(f"🔥 Ошибка на шаге {i} ({symbol}): {str(e)}")
                for o in executed_orders:
                    try:
                        await exchange.cancel_order(o['id'], o['symbol'])
                    except:
                        pass
                return False

        profit_usdt = current_amount - target_volume_usdt
        profit_percent = (profit_usdt / target_volume_usdt) * 100
        
        report = (
            f"✅ <b>РЕАЛЬНАЯ СДЕЛКА</b>\n"
            f"Маршрут: {route_id}\n"
            f"Начальный объем: ${target_volume_usdt:.2f}\n"
            f"Конечный объем: ${current_amount:.2f}\n"
            f"Прибыль: ${profit_usdt:.2f} ({profit_percent:.2f}%)"
        )
        await send_telegram_message(report)
        return True
        
    except Exception as e:
        error_msg = f"🔥 КРИТИЧЕСКАЯ ОШИБКА ТОРГОВЛИ: {str(e)}"
        await send_telegram_message(error_msg)
        return False

async def check_triangle(base, mid1, mid2, symbols, markets):
    try:
        if is_shutting_down:
            return
            
        # Формируем все возможные комбинации пар
        s1_options = [mid1 + base, base + mid1]
        s2_options = [mid1 + mid2, mid2 + mid1]
        s3_options = [mid2 + base, base + mid2]
        
        # Выбираем существующие пары
        s1 = next((s for s in s1_options if s in symbols), None)
        s2 = next((s for s in s2_options if s in symbols), None)
        s3 = next((s for s in s3_options if s in symbols), None)
        
        if not (s1 and s2 and s3):
            return
            
        # Определяем направления торговли в зависимости от формата пары
        # Для s1:
        if s1.endswith(base):  # формат XXXbase
            trade_side1 = 'buy'   # покупаем XXX за base
            price1_factor = 1.0
        else:  # формат baseXXX
            trade_side1 = 'sell'  # продаем base за XXX
            price1_factor = 1.0
            
        # Для s2:
        if s2.endswith(mid1):  # формат XXXmid1
            trade_side2 = 'buy'   # покупаем XXX за mid1
            price2_factor = 1.0
        else:  # формат mid1XXX
            trade_side2 = 'sell'  # продаем mid1 за XXX
            price2_factor = 1.0
            
        # Для s3:
        if s3.endswith(base):  # формат XXXbase
            trade_side3 = 'sell'  # продаем XXX за base
            price3_factor = 1.0
        else:  # формат baseXXX
            trade_side3 = 'buy'   # покупаем base за XXX
            price3_factor = 1.0

        price1, vol1, liq1 = await get_execution_price(s1, trade_side1, target_volume_usdt)
        if not price1: return
        
        # Рассчитываем результат первого шага
        if trade_side1 == 'buy':
            step1 = (1 / price1) * (1 - commission_rate)
        else:
            step1 = price1 * (1 - commission_rate)

        price2, vol2, liq2 = await get_execution_price(s2, trade_side2, target_volume_usdt)
        if not price2: return
        
        # Рассчитываем результат второго шага
        if trade_side2 == 'buy':
            step2 = (1 / price2) * (1 - commission_rate)
        else:
            step2 = price2 * (1 - commission_rate)

        price3, vol3, liq3 = await get_execution_price(s3, trade_side3, target_volume_usdt)
        if not price3: return
        
        # Рассчитываем результат третьего шага
        if trade_side3 == 'buy':
            step3 = (1 / price3) * (1 - commission_rate)
        else:
            step3 = price3 * (1 - commission_rate)

        result = step1 * step2 * step3
        profit_percent = (result - 1) * 100
        if not (min_profit <= profit_percent <= max_profit): 
            return

        route_id = f"{base}->{mid1}->{mid2}->{base}"
        route_hash = hashlib.md5(route_id.encode()).hexdigest()
        now = datetime.utcnow()
        prev_time = triangle_cache.get(route_hash)
        if prev_time and (now - prev_time).total_seconds() >= triangle_hold_time:
            execute = True
        else:
            triangle_cache[route_hash] = now
            execute = False

        min_liquidity = round(min(liq1, liq2, liq3), 2)
        pure_profit_usdt = round((result - 1) * target_volume_usdt, 2)
        valid_pair = True

        # Проверка минимальных параметров для торговли
        trade_ready = execute and valid_pair
        reason = "" if valid_pair else "несуществующая пара"

        message = "\n".join([
            format_line(1, s1, price1, trade_side1, vol1, "green", liq1),
            format_line(2, s2, price2, trade_side2, vol2, "yellow", liq2),
            format_line(3, s3, price3, trade_side3, vol3, "red", liq3),
            "",
            f"💰 Чистая прибыль: {pure_profit_usdt:.2f} USDT",
            f"📈 Спред: {profit_percent:.2f}%",
            f"💧 Мин. ликвидность на шаге: ${min_liquidity}",
            f"⚙️ Готов к сделке: {'ДА' if trade_ready else 'НЕТ'} {reason}",
            f"🔧 Режим: {'РЕАЛЬНАЯ ТОРГОВЛЯ' if real_trading_enabled else 'СИМУЛЯЦИЯ'}"
        ])

        if debug_mode:
            print(message)

        await send_telegram_message(message)
        log_route(base, mid1, mid2, profit_percent, min_liquidity)

        if trade_ready and not is_shutting_down:
            balances = await fetch_balances()
            if balances.get(base, 0) < target_volume_usdt:
                if debug_mode:
                    print(f"[⛔] Недостаточно {base} для входа в сделку")
                return
                
            if real_trading_enabled:
                # Рассчитываем объемы для каждого шага
                amount1 = target_volume_usdt
                amount2 = amount1 / price1 if trade_side1 == 'buy' else amount1 * price1
                amount3 = amount2 / price2 if trade_side2 == 'buy' else amount2 * price2
                
                trade_steps = [
                    (s1, trade_side1, price1, amount1),
                    (s2, trade_side2, price2, amount2),
                    (s3, trade_side3, price3, amount3)
                ]
                
                success = await execute_real_trade(route_id, trade_steps, base, markets)
                if success:
                    print(f"[✅] Реальная сделка по маршруту {route_id} выполнена.")
            else:
                success = await simulate_trading_execution(route_id, profit_percent)
                if success:
                    print(f"[✅] Симулированная сделка по маршруту {route_id} выполнена.")

    except Exception as e:
        if debug_mode:
            print(f"[Ошибка маршрута {base}-{mid1}-{mid2}]: {e}")

async def main():
    try:
        print("Запуск треугольного арбитражного бота")
        print(f"Режим: {'РЕАЛЬНАЯ ТОРГОВЛЯ' if real_trading_enabled else 'СИМУЛЯЦИЯ'}")
        print(f"Целевой объем: {target_volume_usdt} USDT")
        
        symbols, markets = await load_symbols()
        triangles = await find_triangles(symbols)
        
        print(f"🔁 Найдено треугольников: {len(triangles)}")
        if triangles and debug_mode:
            print(f"Примеры треугольников:")
            for i, t in enumerate(triangles[:5]):
                print(f"{i+1}. {t}")
        
        # Тест доступности пар
        test_pair = "BTC/USDT"
        if test_pair in symbols:
            ticker = await exchange.fetch_ticker(test_pair)
            print(f"Тест пары {test_pair}: last={ticker['last']}, bid={ticker['bid']}, ask={ticker['ask']}")
        else:
            print(f"Тестовая пара {test_pair} не найдена")

        await telegram_app.initialize()
        await telegram_app.start()
        await send_telegram_message(f"🚀 Бот запущен! Режим: {'РЕАЛЬНАЯ ТОРГОВЛЯ' if real_trading_enabled else 'СИМУЛЯЦИЯ'}")

        while not is_shutting_down:
            tasks = [check_triangle(base, mid1, mid2, symbols, markets) for base, mid1, mid2 in triangles]
            await asyncio.gather(*tasks)
            await asyncio.sleep(30)
            
    except KeyboardInterrupt:
        print("Остановка по запросу пользователя...")
    except Exception as e:
        print(f"Критическая ошибка: {e}")
        await send_telegram_message(f"🔥 КРИТИЧЕСКАЯ ОШИБКА: {str(e)}")
    finally:
        print("Завершение работы...")
        await send_telegram_message("🛑 Бот остановлен")
        await exchange.close()
        await telegram_app.stop()
        await telegram_app.shutdown()
        print("Ресурсы освобождены")

if __name__ == '__main__':
    asyncio.run(main())
