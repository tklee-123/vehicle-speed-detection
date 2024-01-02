from kafka import KafkaConsumer
import json
import matplotlib.pyplot as plt
import threading

def plot_speed_distribution(consumer, speed_ranges):
    plt.ion()  # Enable Matplotlib's interactive mode

    try:
        for message in consumer:
            data = json.loads(message.value)

            # Extract speed counts from different ranges
            lt20_count = data.get('lt20_count', 0)
            f20t40_count = data.get('f20t40_count', 0)
            f40t60_count = data.get('f40t60_count', 0)
            f60t80_count = data.get('f60t80_count', 0)
            f80t100_count = data.get('f80t100_count', 0)
            gt100_count = data.get('gt100_count', 0)

            # Plot histogram
            plt.clf()
            plt.bar(speed_ranges, [lt20_count, f20t40_count, f40t60_count, f60t80_count, f80t100_count, gt100_count], color=['blue', 'orange', 'green', 'red', 'purple', 'brown'])
            plt.xlabel('Speed Range')
            plt.ylabel('Number of Cars')
            plt.title(f'Speed Distribution - {data["timestamp"]}')
            plt.pause(0.1)

    except KeyboardInterrupt:
        pass
    finally:
        plt.ioff()  # Turn off Matplotlib's interactive mode
        plt.show()

def main():
    topic3 = 'testc'
    consumer3 = KafkaConsumer(
        topic3,
        bootstrap_servers=["localhost:9092"],
        api_version=(0, 10)
    )
    speed_ranges = ['<20', '20-40', '40-60', '60-80', '80-100', '>100']

    consumer_thread3 = threading.Thread(target=plot_speed_distribution,
                                        args=(consumer3, speed_ranges))
    consumer_thread3.start()

    consumer_thread3.join()

if __name__ == "__main__":
    main()
