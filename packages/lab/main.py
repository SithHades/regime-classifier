from training import train_model

def main():
    print("Starting Lab Service...")
    # In a real scenario, we might set up the Cron schedule here or rely on external Cron.
    # The prompt says "Scheduled Execution: Configurable via Cron".
    # Usually this means the script is executed by cron.
    # So we just run the training job once.
    try:
        train_model()
    except Exception as e:
        print(f"Job failed: {e}")

if __name__ == "__main__":
    main()
