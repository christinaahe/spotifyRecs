import pprint


class Song:
    def __init__(self, id, song_info, song_keys):
        self.id = id
        self.info = {song_keys[i]: song_info[i] for i in range(len(song_info))}

    def find_sim_score(self, other, key_weights, thresholds, cat_cols):
        other_info = list(other.info.values())
        info = list(self.info.values())
        key_names = list(self.info.keys())
        sim_components = [key_weights[i] if self.threshold_bool(thresholds[i], info[i], other_info[i])
                          else (-1 * key_weights[i]) for i in range(len(key_weights)) if key_names[i] not in
                          cat_cols]
        return sum(sim_components)

    @staticmethod
    def threshold_bool(threshold, compare_a, compare_b):
        return (float(compare_a) - threshold) <= float(compare_b) <= (float(compare_a) + threshold)

