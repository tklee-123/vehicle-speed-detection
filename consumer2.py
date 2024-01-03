from kafka import KafkaConsumer
import json
import matplotlib.pyplot as plt
import threading

def plot_speed_distribution(consumer, speed_ranges):
    plt.ion()  # Enable Matplotlib's interactive mode

    try:
        land1_counts = {range_: 0 for range_ in speed_ranges}
        land2_counts = {range_: 0 for range_ in speed_ranges}

        for message in consumer:
            data = json.loads(message.value)

            # Extract speed counts for each land
            land = data.get('land', None)
            if land is None:
                continue

            speed_range_counts = {range_: data.get(range_, 0) for range_ in speed_ranges}

            # Update counts based on land
            if land == 'land1':
                land1_counts = speed_range_counts
            elif land == 'land2':
                land2_counts = speed_range_counts

            # Plot histograms for both lands
            plt.clf()
            width = 0.35
            x = range(len(speed_ranges))

            plt.bar(
                [i - width/2 for i in x],
                [land1_counts[range_] for range_ in speed_ranges],
                width,
                label='Land1',
                color='blue'
            )

            plt.bar(
                [i + width/2 for i in x],
                [land2_counts[range_] for range_ in speed_ranges],
                width,
                label='Land2',
                color='orange'
            )

            plt.xlabel('Speed Range')
            plt.ylabel('Number of Cars')
            plt.title(f'Speed Distribution Comparison - {data["timestamp"]}')
            plt.xticks(x, speed_ranges)
            plt.legend()
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
