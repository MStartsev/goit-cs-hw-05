import string
import requests
import matplotlib.pyplot as plt
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor


def load_text(url):
    """Load text from URL"""
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.text
    except requests.RequestException as e:
        raise Exception(f"Failed to download text from {url}: {e}")


def remove_punctuation(text):
    return text.translate(str.maketrans("", "", string.punctuation))


def mapper(word):
    return word, 1


def shuffle(mapped_values):
    shuffled = defaultdict(list)
    for key, value in mapped_values:
        shuffled[key].append(value)
    return shuffled.items()


def reducer(key_values):
    key, values = key_values
    return key, sum(values)


def map_reduce(text):
    # Remove punctuation
    text = remove_punctuation(text)
    words = text.split()

    # Step 1: Parallel Mapping
    with ThreadPoolExecutor() as executor:
        mapped_values = list(executor.map(mapper, words))

    # Step 2: Shuffle
    shuffled_values = shuffle(mapped_values)

    # Step 3: Parallel Reduction
    with ThreadPoolExecutor() as executor:
        reduced_values = list(executor.map(reducer, shuffled_values))

    return dict(reduced_values)


def visualize_top_words(word_counts, top_n=10):
    """Visualize top words by frequency"""
    try:
        sorted_word_counts = sorted(
            word_counts.items(), key=lambda x: x[1], reverse=True
        )[:top_n]
        words, counts = zip(*sorted_word_counts)

        # Creating a figure for the plot with dimensions of 10x5 inches
        plt.figure(figsize=(10, 5))

        # Creating a horizontal bar plot
        plt.barh(words, counts)

        # Adding label to the x-axis
        plt.xlabel("Frequency")

        # Adding label to the y-axis
        plt.ylabel("Words")

        # Adding a title to the plot
        plt.title("Top 10 Most Frequent Words\nJack London: THE GAME")

        # Rotating x-axis and y-axis labels by 15 degrees
        plt.xticks(rotation=15)
        plt.yticks(rotation=15)

        # Reversing the direction of the y-axis (from top to bottom)
        plt.gca().invert_yaxis()

        # Automatically adjusting the plot layout
        plt.tight_layout()

        # Displaying the plot
        plt.show()

    except Exception as e:
        raise Exception(f"Error occurred while visualizing top words: {e}")


def main(url):
    try:
        text = load_text(url)
        if text:
            result = map_reduce(text)

            visualize_top_words(result)

    except Exception as e:
        raise Exception(f"Error occurred in main function: {e}")


if __name__ == "__main__":
    url = "https://www.gutenberg.org/cache/epub/1160/pg1160.txt"

    main(url)
