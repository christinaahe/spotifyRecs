

class Song:
    def __init__(self, song_info, song_keys):
        self.info = {song_keys[i]: song_info[i] for i in range(len(song_info))}

    def find_sim_score(self, other, key_weights, thresholds):
        other_info = other.info.values().to_list()
        info = self.info.values().to_list()
        sim_components = [key_weights[i] if self.threshold_bool(thresholds[i], info[i], other_info[i])
                          else 0 for i in range(len(key_weights))]
        return sum(sim_components)

    @staticmethod
    def threshold_bool(threshold, compare_a, compare_b):
        return (compare_a - threshold) <= compare_b <= (compare_a + threshold)

