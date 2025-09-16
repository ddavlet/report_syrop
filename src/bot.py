# bot.py - Telegram bot for running reports
from __future__ import annotations

import json
import aiohttp
import asyncio
from pathlib import Path
from datetime import datetime
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import Message, FSInputFile, InlineKeyboardButton, InlineKeyboardMarkup, CallbackQuery
from aiogram.enums import ChatAction
from aiogram.client.default import DefaultBotProperties

from src.settings import settings
from src.core import ReportRegistry
import src.reports as reports  # noqa: регистрирует отчёты

# --- доступ по списку user_id (через запятую) ---
_ALLOWED = set()
if settings.telegram_allowed_user_ids:
    _ALLOWED = {int(x.strip()) for x in settings.telegram_allowed_user_ids.split(",") if x.strip()}

bot = Bot(token=settings.telegram_token, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher()

# --- Простая in-memory сессия выбора параметров на пользователя ---
# Формат: { user_id: {"slug": str, "params": dict} }
_USER_STATE: dict[int, dict] = {}




def _escape_html(text: str) -> str:
    """Минимальное экранирование для HTML parse_mode телеграма."""
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


def _md_to_tg_html(text: str) -> str:
    """Очень лёгкая конвертация из простого Markdown в Telegram HTML.

    Поддержка:
    - Заголовки уровня 2-3 (##, ###) → <b>...</b>
    - Жирный **...** → <b>...</b>
    - Инлайн-код `...` → <code>...</code>
    - Маркированные списки, начинающиеся с "- " → заменим маркер на •
    Остальное — как есть, с экранированием HTML.
    """
    import re

    # Экранируем HTML сначала, затем постепенно возвращаем нужные теги
    esc = _escape_html(text)

    # Заголовки ## и ### → выделим жирным
    def repl_h3(m: re.Match) -> str:
        return f"<b>{m.group(1).strip()}</b>\n"

    esc = re.sub(r"^###\s+(.+)$", repl_h3, esc, flags=re.MULTILINE)

    def repl_h2(m: re.Match) -> str:
        return f"<b>{m.group(1).strip()}</b>\n"

    esc = re.sub(r"^##\s+(.+)$", repl_h2, esc, flags=re.MULTILINE)

    # Жирный **...**
    esc = re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", esc)

    # Инлайн-код `...`
    esc = re.sub(r"`([^`]+)`", r"<code>\1</code>", esc)

    # Маркеры списков: "- " в начале строки → "• "
    esc = re.sub(r"^(\s*)-\s+", r"\1• ", esc, flags=re.MULTILINE)

    return esc


def _get_param_presets(slug: str) -> dict[str, list]:
    """Ограниченные варианты значений для каждого отчёта (кнопками)."""
    if slug == "average_check":
        return {
            "dim": [
                "overall",
                "client",
                "month",
                "client_month",
            ],
            "period_days": [15, 30, 60 , 90, 180],
        }
    if slug == "inactive_clients":
        return {
            "cutoff_days": [30, 60, 90, 120],
            "start_date": [f"{datetime.now().year}-01-01", 90, 180, 365],
        }
    if slug == "new_customers":
        return {
            "period_days": [7, 14, 30, 60],
        }
    if slug == "purchase_frequency":
        return {
            "min_orders": [1, 2, 3, 5],
            "period_days": [15, 30, 60 , 90, 180],
        }
    if slug == "abc_clients":
        return {
            "period_days": [15, 30, 60 , 90, 180],
        }
    if slug == "abc_goods":
        return {
            "period_days": [15, 30, 60 , 90, 180],
        }
    # по умолчанию — без параметров
    return {}


def _get_default_params_from_presets(slug: str) -> dict:
    presets = _get_param_presets(slug)
    defaults: dict = {}
    for k, values in presets.items():
        if isinstance(values, list) and values:
            defaults[k] = values[0]
    return defaults


def _render_params_summary(params: dict) -> str:
    if not params:
        return "(без параметров — будут использованы значения по умолчанию)"
    pairs = [f"<code>{k}</code>=<b>{v}</b>" for k, v in params.items()]
    return ", ".join(pairs)


def _build_params_keyboard(slug: str, params: dict) -> InlineKeyboardMarkup:
    presets = _get_param_presets(slug)
    rows: list[list[InlineKeyboardButton]] = []

    # Кнопки выбора параметров
    for key, values in presets.items():
        # для каждого параметра — ряд из значений
        line: list[InlineKeyboardButton] = []
        for v in values:
            is_selected = params.get(key) == v
            label = f"{v}"
            # if label == f"{datetime.now().year}-01-01":
            #     label = "Начало года"
            #     v = f"{datetime.now().year}-01-01"
            if isinstance(v, bool):
                label = "✅ Да" if v else "🚫 Нет"
            if is_selected:
                label = f"[{label}]"
            # Convert datetime objects to strings for JSON serialization
            serializable_v = v
            if isinstance(v, datetime):
                serializable_v = v.isoformat()

            line.append(InlineKeyboardButton(
                text=label,
                callback_data=f"set:{slug}:{key}:{json.dumps(serializable_v)}"
            ))
        rows.append(line)

    # Управляющие кнопки
    rows.append([
        InlineKeyboardButton(text="▶️ Запустить", callback_data=f"do_run:{slug}"),
        InlineKeyboardButton(text="ℹ️ Объяснение", callback_data=f"explain:{slug}"),
    ])
    rows.append([
        InlineKeyboardButton(text="🔁 Сбросить", callback_data=f"reset:{slug}"),
        InlineKeyboardButton(text="⬅️ Назад", callback_data="list_reports"),
    ])

    return InlineKeyboardMarkup(inline_keyboard=rows)


def _check_access(message: Message) -> bool:
    if not _ALLOWED:
        return True  # если список пуст — разрешаем всем
    return message.from_user and message.from_user.id in _ALLOWED


async def _upload_audio_to_endpoint(audio_file_path: str, user_id: int, chat_id: int) -> bool:
    """Upload audio file to the specified endpoint and handle response"""
    try:
        async with aiohttp.ClientSession() as session:
            with open(audio_file_path, 'rb') as audio_file:
                data = aiohttp.FormData()
                data.add_field('audio', audio_file, filename='audio.ogg', content_type='audio/ogg')
                params = {
                    'user_id': user_id,
                    'chat_id': chat_id
                }
                address = 'http://n8n:5678/webhook/6b150169-782c-43ff-ac58-7bc9ac7037da'
                async with session.post(address, params=params,  data=data) as response:
                    if response.status != 200:
                        print(f"Error uploading audio: {response.status}")
                        print(await response.json())
                        return False

                    response_data = await response.json()
                    print(f"Audio uploaded successfully: {response.status}")
                    print(response_data)

                    # Check if the response indicates the report is ready
                    if response_data.get("ready") == True:
                        report_slug = response_data.get("report_slug")
                        parameters = response_data.get("parameters", {})

                        if report_slug:
                            # Generate and send the report
                            await _generate_and_send_report(report_slug, parameters, chat_id)
                            return True
                    else:
                        # If ready is false, send the message field to user
                        message = response_data.get("message")
                        if message:
                            await bot.send_message(chat_id=chat_id, text=message)
                    return True
    except Exception as e:
        print(f"Error uploading audio: {e}")
        return False


async def _generate_and_send_report(report_slug: str, parameters: dict, chat_id: int):
    """Generate and send a report based on the audio response"""
    try:
        # Send typing action to indicate processing
        await bot.send_chat_action(chat_id=chat_id, action=ChatAction.TYPING)

        # Generate the report
        file_path: Path = run_report(report_slug, params=parameters)

        # Send the report file
        await bot.send_chat_action(chat_id=chat_id, action=ChatAction.UPLOAD_DOCUMENT)
        await bot.send_document(
            chat_id=chat_id,
            document=FSInputFile(str(file_path)),
            caption=f"✅ Отчёт готов: <b>{report_slug}</b>\nПараметры: {_render_params_summary(parameters)}\nФайл: <code>{file_path.name}</code>"
        )
    except Exception as e:
        await bot.send_message(
            chat_id=chat_id,
            text=f"❌ Ошибка при генерации отчёта <b>{report_slug}</b>:\n<code>{e}</code>"
        )


def run_report(slug: str, params: dict | None = None) -> Path:
    """Run a report and return the output file path"""
    from src.settings import OUT_DIR

    cls = ReportRegistry.get(slug)
    report = cls(params=params or {})
    df = report.compute()
    out_dir = OUT_DIR / report.slug
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / report.default_filename()
    report.export_excel(df, out_path, title=report.title)
    return out_path


# --- Обработка аудио сообщений
@dp.message(F.audio | F.voice)
async def handle_audio_message(m: Message):
    if not _check_access(m):
        await m.answer("⛔️ Доступ запрещён.")
        return

    try:
        # Получаем файл аудио
        if m.audio:
            file = await bot.get_file(m.audio.file_id)
        elif m.voice:
            file = await bot.get_file(m.voice.file_id)
        else:
            await m.answer("❌ Не удалось получить аудио файл.")
            return

        # Скачиваем файл
        audio_path = f"temp_audio_{m.message_id}.ogg"
        await bot.download_file(file.file_path, audio_path)

        # Отправляем уведомление о начале загрузки
        await m.answer("🎵 Обрабатываю аудио запись...")

        # Загружаем на endpoint
        success = await _upload_audio_to_endpoint(audio_path, m.from_user.id if m.from_user else 0, m.chat.id)

        # Удаляем временный файл
        try:
            Path(audio_path).unlink()
        except:
            pass

        if not success:
            await m.answer("❌ Ошибка при отправке аудио записи на сервер.")

    except Exception as e:
        await m.answer(f"❌ Произошла ошибка при обработке аудио: {str(e)}")


# --- Старт: приветствие и кнопка "📊 Список отчётов"
@dp.message(Command("start"))
async def cmd_start(m: Message):
    if not _check_access(m):
        await m.answer("⛔️ Доступ запрещён.")
        return

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📊 Список отчётов", callback_data="list_reports")]
        ]
    )
    await m.answer(
        "👋 Привет! Я бот для генерации отчётов OK Syrop.\n\n"
        "Нажмите кнопку ниже, чтобы выбрать отчёт.\n\n"
        "🎵 Также вы можете отправить аудио сообщение, и я загружу его на сервер.",
        reply_markup=kb
    )


# --- Кнопка: список отчётов
@dp.callback_query(F.data == "list_reports")
async def cb_list_reports(c: CallbackQuery):
    reports_list = ReportRegistry.all()
    if not reports_list:
        await c.answer("Нет доступных отчётов", show_alert=True)
        return

    rows: list[list[InlineKeyboardButton]] = []
    for slug, cls in reports_list.items():
        rows.append([
            InlineKeyboardButton(text=cls.title or slug, callback_data=f"run_report:{slug}"),
        ])

    kb = InlineKeyboardMarkup(inline_keyboard=rows)
    await c.message.edit_text(
        "📊 <b>Доступные отчёты:</b>\n\nВыберите отчёт для запуска:",
        reply_markup=kb
    )


# --- Кнопка: выбор отчёта (показываем параметры)
@dp.callback_query(F.data.startswith("run_report:"))
async def cb_run_report(c: CallbackQuery):
    slug = c.data.split(":", 1)[1]
    try:
        cls = ReportRegistry.get(slug)
    except KeyError:
        await c.answer(f"Неизвестный отчёт: {slug}", show_alert=True)
        return

    # инициализируем состояние пользователя
    user_id = c.from_user.id if c.from_user else 0
    _USER_STATE[user_id] = {"slug": slug, "params": _get_default_params_from_presets(slug)}
    params = _USER_STATE[user_id]["params"]

    kb = _build_params_keyboard(slug, params)
    await c.message.edit_text(
        f"<b>{cls.title or slug}</b>\n\nВыберите параметры (только кнопки).\nТекущие: {_render_params_summary(params)}",
        reply_markup=kb
    )


# --- Кнопка: установить параметр
@dp.callback_query(F.data.startswith("set:"))
async def cb_set_param(c: CallbackQuery):
    try:
        _, slug, key, raw = c.data.split(":", 3)
    except ValueError:
        await c.answer("Некорректные данные", show_alert=True)
        return

    user_id = c.from_user.id if c.from_user else 0
    state = _USER_STATE.get(user_id)
    if not state or state.get("slug") != slug:
        # если пользователь перескочил — инициализируем
        state = {"slug": slug, "params": _get_default_params_from_presets(slug)}
        _USER_STATE[user_id] = state

    try:
        value = json.loads(raw)
        # Convert ISO datetime strings back to datetime objects
        if isinstance(value, str) and value.count('-') >= 2 and 'T' in value:
            try:
                value = datetime.fromisoformat(value.replace('Z', '+00:00'))
            except ValueError:
                pass  # Keep as string if parsing fails
    except Exception:
        value = raw

    state["params"][key] = value
    kb = _build_params_keyboard(slug, state["params"])
    title = ReportRegistry.get(slug).title or slug
    await c.message.edit_text(
        f"<b>{title}</b>\n\nВыберите параметры (только кнопки).\nТекущие: {_render_params_summary(state['params'])}",
        reply_markup=kb
    )


# --- Кнопка: сброс параметров
@dp.callback_query(F.data.startswith("reset:"))
async def cb_reset_params(c: CallbackQuery):
    slug = c.data.split(":", 1)[1]
    user_id = c.from_user.id if c.from_user else 0
    _USER_STATE[user_id] = {"slug": slug, "params": _get_default_params_from_presets(slug)}
    params = _USER_STATE[user_id]["params"]
    kb = _build_params_keyboard(slug, params)
    title = ReportRegistry.get(slug).title or slug
    await c.message.edit_text(
        f"<b>{title}</b>\n\nПараметры сброшены.\nТекущие: {_render_params_summary(params)}",
        reply_markup=kb
    )


# --- Кнопка: объяснение отчёта (чтение соответствующего .md)
@dp.callback_query(F.data.startswith("explain:"))
async def cb_explain(c: CallbackQuery):
    slug = c.data.split(":", 1)[1]
    # markdown files live alongside code: src/reports/reports/{slug}.md
    md_path = Path(__file__).resolve().parents[0] / "reports" / "reports" / f"{slug}.md"
    if not md_path.exists():
        await c.answer("Описание не найдено", show_alert=True)
        return
    try:
        text = md_path.read_text(encoding="utf-8")
    except Exception as e:
        await c.answer(f"Не удалось прочитать .md: {e}", show_alert=True)
        return
    # Отправим отдельным сообщением, не меняя экран параметров
    html = _md_to_tg_html(text)
    await c.message.answer(html)


# --- Кнопка: запуск отчёта с выбранными параметрами
@dp.callback_query(F.data.startswith("do_run:"))
async def cb_do_run(c: CallbackQuery):
    slug = c.data.split(":", 1)[1]

    try:
        ReportRegistry.get(slug)
    except KeyError:
        await c.answer(f"Неизвестный отчёт: {slug}", show_alert=True)
        return

    user_id = c.from_user.id if c.from_user else 0
    params = (_USER_STATE.get(user_id) or {}).get("params") or {}

    await bot.send_chat_action(chat_id=c.message.chat.id, action=ChatAction.TYPING)
    await c.message.answer(f"⏳ Запускаю отчёт <b>{slug}</b> с параметрами: {_render_params_summary(params)}")

    try:
        file_path: Path = run_report(slug, params=params)
        await bot.send_chat_action(chat_id=c.message.chat.id, action=ChatAction.UPLOAD_DOCUMENT)
        await c.message.answer_document(
            document=FSInputFile(str(file_path)),
            caption=f"✅ Готово: <b>{slug}</b>\nФайл: <code>{file_path.name}</code>"
        )
    except Exception as e:
        await c.message.answer(f"❌ Ошибка при выполнении отчёта:\n<code>{e}</code>")


def main():
    """Run the Telegram bot"""
    import asyncio
    print("🤖 Starting Telegram bot...")
    asyncio.run(dp.start_polling(bot))


if __name__ == "__main__":
    main()
