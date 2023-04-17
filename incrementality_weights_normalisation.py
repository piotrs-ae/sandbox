from collections import defaultdict

def normalize_weights_by_granularity(weights, granularity_key_func):
    # Group weights by granularity
    weights_by_granularity = defaultdict(list)
    for weight_tuple in weights:
        granularity_key = granularity_key_func(weight_tuple)
        weights_by_granularity[granularity_key].append(weight_tuple)

    # Normalize weights separately for each granularity level
    normalized_weights = []
    for granularity_key, granularity_weights in weights_by_granularity.items():
        total_weight = sum([weight for *_, weight in granularity_weights])
        granularity_normalized_weights = [(dim1, dim2, dim3, weight / total_weight) for dim1, dim2, dim3, weight in granularity_weights]
        normalized_weights.extend(granularity_normalized_weights)

    return normalized_weights

# Example usage:
weights = [
    ('Symbian OS', 'UA', 'TikTok', 10),
    ('Android', 'PL', 'GAC', 20),
    ('Chrome OS', 'RO', 'Meta', 30),
    ('Symbian OS', 'UA', 'Instagram', 40),
    ('Android', 'PL', 'Linkedin', 50),
    ('Chrome OS', 'RO', 'Other', 60),
]

# Define a function that returns the granularity key based on the dimensions
def granularity_key_func(weight_tuple):
    dim1, dim2, dim3, _ = weight_tuple
    granularity_key = (dim1, dim2)  # change based on granularity set up
    return granularity_key

normalized_weights = normalize_weights_by_granularity(weights, granularity_key_func)
