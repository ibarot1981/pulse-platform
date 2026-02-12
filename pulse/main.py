from telegram.ext import ApplicationBuilder
from pulse.config import BOT_TOKEN


def main():

    app = ApplicationBuilder().token(BOT_TOKEN).build()

    # Add handlers here
    # Menu handlers
    # Task handlers
    # Reminder scheduler

    print("Pulse running...")

    app.run_polling()


if __name__ == "__main__":
    main()
