import json
from pathlib import Path

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import Message, FSInputFile, InlineKeyboardButton, InlineKeyboardMarkup, CallbackQuery
from aiogram.enums import ChatAction
from aiogram.client.default import DefaultBotProperties

from settings import settings
from core import ReportRegistry
from runner import run_report
import reports  # noqa — автодискавери, регистрация отчётов

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
                "price_type",
                "month",
                "client_month",
                "price_type_month",
            ],
            "period_days": [7, 14, 30, 60],
        }
    if slug == "declined_flavors":
        return {
            "recent_days": [14, 30, 60],
            "baseline_days": [60, 90, 180],
            "min_item_orders_base": [1, 3, 5],
            "min_client_orders_recent": [1, 2, 3],
            "min_drop_pct": [30, 50, 70],
        }
    if slug == "inactive_clients":
        return {
            "cutoff_days": [30, 60, 90, 120],
        }
    if slug == "new_customers":
        return {
            "period_days": [7, 14, 30, 60],
        }
    if slug == "purchase_frequency":
        return {
            "min_orders": [1, 2, 3, 5],
            "period_days": [7, 14, 30, 60],
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
            if isinstance(v, bool):
                label = "✅ Да" if v else "🚫 Нет"
            if is_selected:
                label = f"[{label}]"
            line.append(InlineKeyboardButton(
                text=label,
                callback_data=f"set:{slug}:{key}:{json.dumps(v)}"
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

    await m.answer("Привет! Я бот для генерации отчётов.\nНажми кнопку ниже:", reply_markup=kb)


# --- Кнопка: список отчётов
@dp.callback_query(F.data == "list_reports")
async def cb_list_reports(c: CallbackQuery):
    reports_list = ReportRegistry.all()
    if not reports_list:
        await c.message.edit_text("Пока нет доступных отчётов.")
        return

    # Кнопки для каждого отчёта
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=cls.title or slug, callback_data=f"run_report:{slug}")]
            for slug, cls in reports_list.items()
        ]
    )
    await c.message.edit_text("Выбери отчёт:", reply_markup=kb)


# --- Кнопка: выбор конкретного отчёта → экран параметров
@dp.callback_query(F.data.startswith("run_report:"))
async def cb_run_report(c: CallbackQuery):
    slug = c.data.split(":", 1)[1]

    try:
        cls = ReportRegistry.get(slug)
    except KeyError:
        await c.answer(f"Неизвестный отчёт: {slug}", show_alert=True)
        return

    user_id = c.from_user.id if c.from_user else 0
    # инициализируем состояние с дефолтными параметрами
    _USER_STATE[user_id] = {
        "slug": slug,
        "params": _get_default_params_from_presets(slug),
    }

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
    md_path = Path(__file__).parent / "reports" / f"{slug}.md"
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
    import asyncio
    asyncio.run(dp.start_polling(bot))


if __name__ == "__main__":
    main()
