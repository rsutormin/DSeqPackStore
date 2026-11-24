use crate::{Kmer, KmerMode};
use ahash::AHashMap;
use serde::{Deserialize, Serialize};
use std::cmp::Reverse;

#[derive(Default, Serialize, Deserialize)]
pub struct SplitterIndex {
    names: Vec<String>,
    profile_keys: Vec<Vec<u64>>,
    slots: Vec<Option<(u64, Vec<usize>)>>,
    keys: usize,
}

#[derive(Default)]
struct Score {
    covered: usize,
    hits: usize,
    rare_votes: usize,
    contig: usize,
    end: usize,
}

pub struct Match {
    pub name: Option<String>,
    pub coverage: f64,
}

fn hash(mut key: u64) -> usize {
    key = (key ^ (key >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    key = (key ^ (key >> 27)).wrapping_mul(0x94d049bb133111eb);
    (key ^ (key >> 31)) as usize
}

impl SplitterIndex {
    fn slot(&self, key: u64) -> usize {
        let mask = self.slots.len() - 1;
        let mut slot = hash(key) & mask;
        while self.slots[slot].as_ref().is_some_and(|(stored, _)| *stored != key) {
            slot = (slot + 1) & mask;
        }
        slot
    }

    pub fn add_profile(&mut self, name: String, splitters: &[u64]) {
        let id = self.names.len();
        self.names.push(name);
        self.profile_keys.push(Vec::new());
        self.insert_keys(id, splitters);
    }

    pub fn extend_profile(&mut self, name: &str, splitters: &[u64]) {
        let id = self.names.iter().position(|entry| entry == name).expect("Profile must be indexed");
        self.insert_keys(id, splitters);
    }

    fn insert_keys(&mut self, id: usize, splitters: &[u64]) {
        self.profile_keys[id].extend_from_slice(splitters);
        for &key in splitters {
            if (self.keys + 1) * 2 > self.slots.len() {
                let capacity = (self.slots.len() * 2).max(16);
                let old = std::mem::replace(&mut self.slots, vec![None; capacity]);
                for (key, ids) in old.into_iter().flatten() {
                    let slot = self.slot(key);
                    self.slots[slot] = Some((key, ids));
                }
            }
            let slot = self.slot(key);
            match &mut self.slots[slot] {
                Some((_, ids)) if ids.last() != Some(&id) => ids.push(id),
                Some(_) => {}
                empty => {
                    *empty = Some((key, vec![id]));
                    self.keys += 1;
                }
            }
        }
    }

    pub fn select_two_stage<'a>(
        &self, contigs: impl IntoIterator<Item = &'a [u8]>, k: usize, segment_size: usize,
        minimum_coverage: f64,
    ) -> Match {
        let contigs: Vec<_> = contigs.into_iter().collect();
        let (_, scores) = self.score(&contigs, k, segment_size, true, None);
        let mut ranked: Vec<_> = scores.into_iter().collect();
        ranked.sort_unstable_by(|(a, sa), (b, sb)| {
            (sb.covered, sb.rare_votes, sb.hits).cmp(&(sa.covered, sa.rare_votes, sa.hits))
                .then_with(|| self.names[*a].cmp(&self.names[*b]))
        });
        let chosen = &ranked[..ranked.len().min(4)];
        // Sparse samples or borderline coverage require an exact search over the index.
        if chosen.first().is_none_or(|(_, score)| score.hits < 3) {
            return self.score(&contigs, k, segment_size, false, None).0;
        }

        let mut shortlist = AHashMap::<u64, Vec<usize>>::new();
        for &(id, _) in chosen {
            for &key in &self.profile_keys[id] {
                shortlist.entry(key).or_default().push(id);
            }
        }
        let exact = self.score(&contigs, k, segment_size, false, Some(&shortlist)).0;
        if exact.coverage < minimum_coverage + 0.05 {
            return self.score(&contigs, k, segment_size, false, None).0;
        }
        exact
    }

    fn score(
        &self, contigs: &[&[u8]], k: usize, segment_size: usize, sampled: bool,
        shortlist: Option<&AHashMap<u64, Vec<usize>>>,
    ) -> (Match, AHashMap<usize, Score>) {
        let mut scores = AHashMap::<usize, Score>::new();
        let mut total = 0;
        let radius = segment_size / 2;
        for (contig_no, contig) in contigs.iter().enumerate() {
            total += contig.len();
            if self.slots.is_empty() {
                continue;
            }
            let mut kmer = Kmer::new(k as u32, KmerMode::Canonical);
            for (i, &base) in contig.iter().enumerate() {
                if base > 3 {
                    kmer.reset();
                    continue;
                }
                kmer.insert(base as u64);
                if !kmer.is_full() {
                    continue;
                }
                let key = kmer.data();
                let ids = if let Some(shortlist) = shortlist {
                    shortlist.get(&key)
                } else {
                    if sampled && hash(key) >> (usize::BITS - 3) != 0 {
                        continue;
                    }
                    self.slots[self.slot(key)].as_ref().map(|(_, ids)| ids)
                };
                if let Some(ids) = ids {
                    let pos = i + 1;
                    let start = pos.saturating_sub(radius);
                    let end = (pos + radius).min(contig.len());
                    for &id in ids {
                        let score = scores.entry(id).or_default();
                        if score.contig != contig_no {
                            score.contig = contig_no;
                            score.end = 0;
                        }
                        score.covered += end.saturating_sub(start.max(score.end));
                        score.end = end;
                        score.hits += 1;
                        if sampled {
                            score.rare_votes += 1_000_000 / ids.len();
                        }
                    }
                }
            }
        }
        let best = scores.iter().max_by_key(|&(&id, score)| {
            (score.covered, score.hits, Reverse(self.names[id].as_str()))
        });
        let result = Match {
            name: best.map(|(&id, _)| self.names[id].clone()),
            coverage: best.map_or(0.0, |(_, score)| score.covered as f64 / total.max(1) as f64),
        };
        (result, scores)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expanded_profile_remains_searchable_after_checkpoint_round_trip() {
        let input = [0, 1, 2, 3];
        let mut anchor = Kmer::new(4, KmerMode::Canonical);
        for base in input {
            anchor.insert(base.into());
        }

        let mut index = SplitterIndex::default();
        index.add_profile("founder".into(), &[]);
        assert!(index.select_two_stage([input.as_slice()], 4, 8, 0.5).name.is_none());

        index.extend_profile("founder", &[anchor.data()]);
        let bytes = bincode::serialize(&index).unwrap();
        let restored: SplitterIndex = bincode::deserialize(&bytes).unwrap();
        let matched = restored.select_two_stage([input.as_slice()], 4, 8, 0.5);
        assert_eq!(matched.name.as_deref(), Some("founder"));
        assert_eq!(matched.coverage, 1.0);
    }
}
