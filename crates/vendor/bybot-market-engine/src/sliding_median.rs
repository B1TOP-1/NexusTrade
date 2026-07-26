use std::collections::VecDeque;

#[derive(Debug, Clone)]
pub struct SlidingMedian {
    window_size: usize,
    queue: VecDeque<f64>,
    sorted: Vec<f64>,
}

impl SlidingMedian {
    pub fn new(window_size: usize) -> Self {
        Self {
            window_size,
            queue: VecDeque::with_capacity(window_size),
            sorted: Vec::with_capacity(window_size),
        }
    }

    pub fn push(&mut self, value: f64) {
        if self.window_size == 0 {
            return;
        }
        if self.queue.len() == self.window_size {
            if let Some(old_value) = self.queue.pop_front() {
                if let Ok(index) = self.sorted.binary_search_by(|probe| probe.partial_cmp(&old_value).unwrap()) {
                    self.sorted.remove(index);
                }
            }
        }
        let index = self
            .sorted
            .binary_search_by(|probe| probe.partial_cmp(&value).unwrap())
            .unwrap_or_else(|index| index);
        self.sorted.insert(index, value);
        self.queue.push_back(value);
    }

    pub fn len(&self) -> usize {
        self.queue.len()
    }

    pub fn median(&self) -> f64 {
        let len = self.sorted.len();
        if len == 0 {
            0.0
        } else if len % 2 == 1 {
            self.sorted[len / 2]
        } else {
            (self.sorted[len / 2 - 1] + self.sorted[len / 2]) / 2.0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_window_median_updates_without_resorting_history() {
        let mut median = SlidingMedian::new(3);

        median.push(3.0);
        median.push(1.0);
        median.push(2.0);

        assert_eq!(median.len(), 3);
        assert_eq!(median.median(), 2.0);

        median.push(10.0);

        assert_eq!(median.len(), 3);
        assert_eq!(median.median(), 2.0);
    }

    #[test]
    fn even_window_uses_average_of_middle_values() {
        let mut median = SlidingMedian::new(4);
        median.push(1.0);
        median.push(4.0);
        median.push(2.0);
        median.push(3.0);

        assert_eq!(median.median(), 2.5);
    }
}
