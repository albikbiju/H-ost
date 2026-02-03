import asyncio
import logging
import os
import sys
import subprocess
import psutil
import hashlib
import ast
from datetime import datetime
from typing import Dict, Optional
import json
import shutil 

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery

# ==============================================================================
# CONFIGURATION
# ==============================================================================

# ✅ FIXED: Now reads token from Railway environment variable
TOKEN = os.environ.get("TOKEN", "8123942580:AAEnSdMm3L_gN87UjDBHIUOaW4xlTs_S9zg")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('hosting_bot.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

HOSTED_BOTS_DIR = 'hosted_bots'
BOTS_DB_FILE = 'hosted_bots_db.json'
os.makedirs(HOSTED_BOTS_DIR, exist_ok=True)

# States
class BotUpload(StatesGroup):
    waiting_for_file = State()

# ==============================================================================
# HOSTED BOT MANAGER
# ==============================================================================
class HostedBot:
    def __init__(self, user_id: int, bot_hash: str, file_name: str):
        self.user_id = user_id
        self.bot_hash = bot_hash
        self.file_name = file_name
        self.bot_dir = os.path.join(HOSTED_BOTS_DIR, f"{user_id}_{bot_hash}")
        self.script_path = os.path.join(self.bot_dir, file_name)
        self.requirements_path = os.path.join(self.bot_dir, 'requirements.txt')

        # Path logic for Virtual Environment (Windows vs Linux)
        self.venv_path = os.path.join(self.bot_dir, 'venv')
        if sys.platform == "win32":
            self.venv_python = os.path.join(self.venv_path, 'Scripts', 'python.exe')
            self.venv_pip = os.path.join(self.venv_path, 'Scripts', 'pip.exe')
        else:
            self.venv_python = os.path.join(self.venv_path, 'bin', 'python')
            self.venv_pip = os.path.join(self.venv_path, 'bin', 'pip')

        self.process: Optional[subprocess.Popen] = None
        self.pid: Optional[int] = None
        self.status = "stopped"
        self.created_at = datetime.now().isoformat()
        self.started_at: Optional[str] = None
        self.stopped_at: Optional[str] = None
        self.crashes = 0
        self.dependencies_installed = False

        os.makedirs(self.bot_dir, exist_ok=True)

    def to_dict(self):
        return {
            'user_id': self.user_id,
            'bot_hash': self.bot_hash,
            'file_name': self.file_name,
            'status': self.status,
            'created_at': self.created_at,
            'started_at': self.started_at,
            'stopped_at': self.stopped_at,
            'crashes': self.crashes,
            'pid': self.pid,
            'dependencies_installed': self.dependencies_installed
        }

    @classmethod
    def from_dict(cls, data):
        bot = cls(data['user_id'], data['bot_hash'], data['file_name'])
        bot.status = data.get('status', 'stopped')
        bot.created_at = data.get('created_at', datetime.now().isoformat())
        bot.started_at = data.get('started_at')
        bot.stopped_at = data.get('stopped_at')
        bot.crashes = data.get('crashes', 0)
        bot.pid = data.get('pid')
        bot.dependencies_installed = data.get('dependencies_installed', False)
        return bot

    def extract_imports(self):
        try:
            with open(self.script_path, 'r', encoding='utf-8') as f:
                content = f.read()
            tree = ast.parse(content)
            imports = set()
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    for alias in node.names:
                        imports.add(alias.name.split('.')[0])
                elif isinstance(node, ast.ImportFrom):
                    if node.module:
                        imports.add(node.module.split('.')[0])
            stdlib_modules = set(sys.stdlib_module_names)
            external_imports = imports - stdlib_modules - {'__future__'}
            return list(external_imports)
        except Exception as e:
            logger.error(f"Error extracting imports: {e}")
            return []

    def create_requirements(self):
        imports = self.extract_imports()
        package_mapping = {
            'aiogram': 'aiogram',
            'telegram': 'python-telegram-bot',
            'telebot': 'pyTelegramBotAPI',
            'cv2': 'opencv-python',
            'PIL': 'Pillow',
            'sklearn': 'scikit-learn',
            'yaml': 'PyYAML',
            'discord': 'discord.py',
            'requests': 'requests',
            'numpy': 'numpy'
        }
        packages = []
        for imp in imports:
            package = package_mapping.get(imp, imp)
            packages.append(package)

        if packages:
            with open(self.requirements_path, 'w') as f:
                f.write('\n'.join(packages))

    async def create_venv(self):
        """Creates a virtual environment for the bot"""
        if os.path.exists(self.venv_python):
            return True, "Venv already exists"

        try:
            # Use sys.executable to ensure we use the same python version
            process = await asyncio.create_subprocess_exec(
                sys.executable, '-m', 'venv', self.venv_path,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await process.communicate()

            if process.returncode == 0:
                return True, "Venv created"
            else:
                return False, stderr.decode()
        except Exception as e:
            return False, str(e)

    async def install_dependencies(self):
        """Install dependencies from requirements.txt INTO THE VENV"""
        if not os.path.exists(self.requirements_path):
            self.dependencies_installed = True
            return True, "No dependencies to install"

        success, msg = await self.create_venv()
        if not success:
            return False, f"Failed to create environment: {msg}"

        try:
            cmd = [self.venv_pip, 'install', '-r', 'requirements.txt']

            process = await asyncio.create_subprocess_exec(
                *cmd,
                cwd=self.bot_dir,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )

            stdout, stderr = await process.communicate()

            if process.returncode == 0:
                self.dependencies_installed = True
                return True, "Dependencies installed successfully"
            else:
                error_msg = stderr.decode()
                logger.error(f"Dependency installation failed: {error_msg}")
                return False, f"Installation failed: {error_msg[-200:]}"

        except Exception as e:
            logger.error(f"Error installing dependencies: {e}")
            return False, f"Error: {str(e)}"

    async def start(self):
        """Start the bot script"""
        if self.status == "running":
            return False, "Bot is already running"

        if not os.path.exists(self.script_path):
            return False, "Bot script not found"

        if not os.path.exists(self.venv_python):
             await self.create_venv()

        if not self.dependencies_installed and os.path.exists(self.requirements_path):
            success, message = await self.install_dependencies()
            if not success:
                return False, f"Dependency installation failed: {message}"

        try:
            # Platform specific flags
            kwargs = {}
            if sys.platform != "win32":
                kwargs['start_new_session'] = True

            self.process = subprocess.Popen(
                [self.venv_python, self.file_name],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                cwd=self.bot_dir,
                **kwargs
            )

            self.pid = self.process.pid
            self.status = "running"
            self.started_at = datetime.now().isoformat()

            logger.info(f"Bot started: {self.file_name} (PID: {self.pid})")
            return True, f"✅ Bot started successfully!\nPID: {self.pid}"

        except Exception as e:
            logger.error(f"Failed to start bot: {e}")
            self.status = "error"
            self.crashes += 1
            return False, f"Failed to start: {str(e)}"

    async def stop(self):
        """Stop the bot process"""
        if self.status != "running":
            return False, "Bot is not running"

        try:
            if self.pid:
                try:
                    process = psutil.Process(self.pid)
                    for child in process.children(recursive=True):
                        child.terminate()
                    process.terminate()
                    try:
                        process.wait(timeout=5)
                    except psutil.TimeoutExpired:
                        for child in process.children(recursive=True):
                            child.kill()
                        process.kill()
                except psutil.NoSuchProcess:
                    pass

            if self.process:
                self.process.terminate()

            self.status = "stopped"
            self.stopped_at = datetime.now().isoformat()
            self.pid = None
            return True, "✅ Bot stopped successfully"

        except Exception as e:
            return False, f"Failed to stop: {str(e)}"

    async def restart(self):
        await self.stop()
        await asyncio.sleep(1)
        return await self.start()

    async def check_status(self):
        if self.status == "running" and self.pid:
            try:
                process = psutil.Process(self.pid)
                if not process.is_running() or process.status() == psutil.STATUS_ZOMBIE:
                    self.status = "crashed"
                    self.crashes += 1
                    self.stopped_at = datetime.now().isoformat()
            except psutil.NoSuchProcess:
                self.status = "crashed"
                self.crashes += 1
                self.stopped_at = datetime.now().isoformat()

    def get_uptime(self):
        if self.status != "running" or not self.started_at:
            return "N/A"
        started = datetime.fromisoformat(self.started_at)
        uptime = datetime.now() - started
        hours, remainder = divmod(int(uptime.total_seconds()), 3600)
        minutes, seconds = divmod(remainder, 60)
        return f"{hours}h {minutes}m {seconds}s"

# ==============================================================================
# DATABASE MANAGER
# ==============================================================================
class BotDatabase:
    def __init__(self):
        self.bots: Dict[str, HostedBot] = {}
        self.load()

    def load(self):
        if os.path.exists(BOTS_DB_FILE):
            try:
                with open(BOTS_DB_FILE, 'r') as f:
                    data = json.load(f)
                    for key, bot_data in data.items():
                        self.bots[key] = HostedBot.from_dict(bot_data)
                logger.info(f"Loaded {len(self.bots)} bots from database")
            except Exception as e:
                logger.error(f"Failed to load database: {e}")

    def save(self):
        try:
            data = {key: bot.to_dict() for key, bot in self.bots.items()}
            with open(BOTS_DB_FILE, 'w') as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save database: {e}")

    def add_bot(self, bot: HostedBot):
        key = f"{bot.user_id}_{bot.bot_hash}"
        self.bots[key] = bot
        self.save()

    def get_bot(self, user_id: int, bot_hash: str) -> Optional[HostedBot]:
        key = f"{user_id}_{bot_hash}"
        return self.bots.get(key)

    def get_user_bots(self, user_id: int):
        return [bot for bot in self.bots.values() if bot.user_id == user_id]

    def remove_bot(self, user_id: int, bot_hash: str):
        key = f"{user_id}_{bot_hash}"
        if key in self.bots:
            del self.bots[key]
            self.save()

db = BotDatabase()

# ==============================================================================
# UI HELPERS
# ==============================================================================
def get_main_keyboard():
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="📤 Upload Bot", callback_data="upload_bot"),
            InlineKeyboardButton(text="📋 My Bots", callback_data="my_bots")
        ],
        [InlineKeyboardButton(text="📊 Statistics", callback_data="stats")]
    ])
    return keyboard

def get_bot_control_keyboard(bot_hash: str, status: str):
    buttons = []
    if status == "running":
        buttons.append([
            InlineKeyboardButton(text="⏹ Stop", callback_data=f"stop_{bot_hash}"),
            InlineKeyboardButton(text="🔄 Restart", callback_data=f"restart_{bot_hash}")
        ])
    else:
        buttons.append([
            InlineKeyboardButton(text="▶️ Start", callback_data=f"start_{bot_hash}"),
            InlineKeyboardButton(text="🔄 Restart", callback_data=f"restart_{bot_hash}")
        ])
    buttons.extend([
        [InlineKeyboardButton(text="🗑 Delete", callback_data=f"delete_{bot_hash}")]
    ])
    buttons.append([InlineKeyboardButton(text="« Back", callback_data="my_bots")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

# ==============================================================================
# HANDLERS
# ==============================================================================
async def cmd_start(message: types.Message):
    await message.answer(
        "🤖 <b>Bot Hosting Service</b>\nUpload .py files to host them.",
        reply_markup=get_main_keyboard(),
        parse_mode="HTML"
    )

async def callback_upload_bot(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text("📤 Send me your <b>.py</b> file.", parse_mode="HTML")
    await state.set_state(BotUpload.waiting_for_file)
    await callback.answer()

async def callback_my_bots(callback: CallbackQuery):
    user_bots = db.get_user_bots(callback.from_user.id)
    if not user_bots:
        await callback.message.edit_text("You have no bots.", reply_markup=get_main_keyboard())
        await callback.answer()
        return

    for bot in user_bots:
        await bot.check_status()
    db.save()

    text = "📋 <b>Your Hosted Bots</b>\n"
    buttons = []
    for bot in user_bots:
        status_icon = "🟢" if bot.status == "running" else "🔴"
        buttons.append([InlineKeyboardButton(
            text=f"{status_icon} {bot.file_name}", 
            callback_data=f"view_{bot.bot_hash}"
        )])
    buttons.append([InlineKeyboardButton(text="« Back", callback_data="main_menu")])

    await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="HTML")
    await callback.answer()

async def callback_view_bot(callback: CallbackQuery):
    try:
        bot_hash = callback.data.split("_")[1]
        bot = db.get_bot(callback.from_user.id, bot_hash)
        if not bot:
            await callback.answer("Bot not found.", show_alert=True)
            return

        await bot.check_status()
        text = f"🤖 <b>{bot.file_name}</b>\nStatus: {bot.status}\nUptime: {bot.get_uptime()}"
        await callback.message.edit_text(text, reply_markup=get_bot_control_keyboard(bot_hash, bot.status), parse_mode="HTML")
        await callback.answer()
    except IndexError:
        await callback.answer("Error viewing bot.")

async def callback_action_bot(callback: CallbackQuery):
    action, bot_hash = callback.data.split("_", 1)
    bot = db.get_bot(callback.from_user.id, bot_hash)
    if not bot: 
        await callback.answer("Bot not found", show_alert=True)
        return

    if action == "start":
        await callback.answer("Starting...", show_alert=False)
        success, msg = await bot.start()
        await callback.answer(msg, show_alert=True)
    elif action == "stop":
        await callback.answer("Stopping...", show_alert=False)
        success, msg = await bot.stop()
        await callback.answer(msg, show_alert=True)
    elif action == "restart":
        await callback.answer("Restarting...", show_alert=False)
        success, msg = await bot.restart()
        await callback.answer(msg, show_alert=True)
    elif action == "delete":
        if bot.status == "running": 
            await bot.stop()
        if os.path.exists(bot.bot_dir):
            try:
                shutil.rmtree(bot.bot_dir)
            except Exception as e:
                logger.error(f"Error deleting dir: {e}")
        db.remove_bot(callback.from_user.id, bot_hash)
        await callback.answer("Deleted", show_alert=True)
        await callback_my_bots(callback)
        return

    db.save()
    await callback_view_bot(callback)

async def callback_main_menu(callback: CallbackQuery):
    await callback.message.edit_text(
        "🤖 <b>Bot Hosting Service</b>\nUpload .py files to host them.",
        reply_markup=get_main_keyboard(),
        parse_mode="HTML"
    )
    await callback.answer()

async def handle_document(message: types.Message, state: FSMContext):
    document = message.document
    if not document.file_name.endswith('.py'):
        await message.answer("❌ Only .py files allowed.")
        return

    file = await message.bot.get_file(document.file_id)
    file_content = await message.bot.download_file(file.file_path)
    content_bytes = file_content.read()

    bot_hash = hashlib.md5(content_bytes).hexdigest()[:16]

    hosted_bot = HostedBot(message.from_user.id, bot_hash, document.file_name)

    with open(hosted_bot.script_path, 'wb') as f:
        f.write(content_bytes)

    hosted_bot.create_requirements()
    db.add_bot(hosted_bot)

    await message.answer("✅ Uploaded! Creating environment...", reply_markup=get_bot_control_keyboard(bot_hash, "stopped"))
    await hosted_bot.create_venv()
    await state.clear()

async def monitor_bots():
    while True:
        try:
            if db.bots:
                for bot in list(db.bots.values()): 
                    await bot.check_status()
                db.save()
        except Exception as e:
            logger.error(f"Monitor error: {e}")
        await asyncio.sleep(30)

async def on_shutdown():
    logger.info("Shutting down...")
    for bot in db.bots.values():
        if bot.status == "running":
            await bot.stop()

async def main():
    if not TOKEN:
        logger.error("❌ ERROR: Bot Token is missing.")
        return

    bot = Bot(token=TOKEN)
    dp = Dispatcher(storage=MemoryStorage())

    dp.message.register(cmd_start, Command("start"))
    dp.callback_query.register(callback_upload_bot, F.data == "upload_bot")
    dp.callback_query.register(callback_my_bots, F.data == "my_bots")
    dp.callback_query.register(callback_main_menu, F.data == "main_menu")
    dp.callback_query.register(callback_view_bot, F.data.startswith("view_"))
    dp.callback_query.register(callback_action_bot, F.data.startswith(("start_", "stop_", "restart_", "delete_")))
    dp.message.register(handle_document, F.document, BotUpload.waiting_for_file)

    asyncio.create_task(monitor_bots())

    try:
        logger.info("✅ Bot is running...")
        await dp.start_polling(bot)
    finally:
        await on_shutdown()
        await bot.session.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
