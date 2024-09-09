use std::{cmp::Ordering, iter, mem::replace, num::NonZeroU16};

#[derive(Debug)]
pub struct BufferWeightMap {
    // An minheap (in array representation) consisting of buffer indices ordered by their
    // corresponding buffer weights.
    heap_index_to_buffer_index: Vec<u16>,

    // The minimum weight of a buffer is 1.
    //
    // The maximum weight of a buffer is 8419, which is the number of intermediate symbols
    // corresponding to 8192 source symbols, 8192 being the maximum number of source symbols.
    buffer_index_to_weight: Vec<Option<NonZeroU16>>,

    buffer_index_to_heap_index: Vec<u16>,
}

impl BufferWeightMap {
    pub fn new(num_expected_buffers: usize) -> BufferWeightMap {
        BufferWeightMap {
            heap_index_to_buffer_index: Vec::with_capacity(num_expected_buffers),
            buffer_index_to_weight: Vec::with_capacity(num_expected_buffers),
            buffer_index_to_heap_index: Vec::with_capacity(num_expected_buffers),
        }
    }

    fn print_node(&self, heap_index: usize, indent: usize) {
        if heap_index < self.heap_index_to_buffer_index.len() {
            let buffer_index = self.heap_index_to_buffer_index[heap_index];
            let weight = self.buffer_index_to_weight[usize::from(buffer_index)];

            for _ in 0..indent {
                print!("    ");
            }
            println!("{}: {} {:?}", heap_index, buffer_index, weight);

            self.print_node(2 * heap_index + 1, indent + 1);
            self.print_node(2 * heap_index + 2, indent + 1);
        }
    }

    #[allow(dead_code)]
    fn print(&self) {
        self.print_node(0, 0);
        println!();
    }

    pub fn check_force(&self) {
        let mut buffer_index_to_weight: Vec<_> = iter::repeat(None)
            .take(self.buffer_index_to_weight.len())
            .collect();

        for buffer_index in &self.heap_index_to_buffer_index {
            let buffer_index = usize::from(*buffer_index);
            buffer_index_to_weight[buffer_index] = self.buffer_index_to_weight[buffer_index];
        }

        assert_eq!(self.buffer_index_to_weight, buffer_index_to_weight);

        for i in 0..self.heap_index_to_buffer_index.len() {
            let weight = self.buffer_index_to_weight
                [usize::from(self.heap_index_to_buffer_index[i])]
            .unwrap();

            let child = 2 * i + 1;
            if child < self.heap_index_to_buffer_index.len() {
                let child_weight = self.buffer_index_to_weight
                    [usize::from(self.heap_index_to_buffer_index[child])]
                .unwrap();
                assert!(weight <= child_weight);
            }

            let child = 2 * i + 2;
            if child < self.heap_index_to_buffer_index.len() {
                let child_weight = self.buffer_index_to_weight
                    [usize::from(self.heap_index_to_buffer_index[child])]
                .unwrap();
                assert!(weight <= child_weight);
            }
        }

        for (i, buffer_index) in self.heap_index_to_buffer_index.iter().enumerate() {
            let buffer_index = usize::from(*buffer_index);

            assert_eq!(
                usize::from(self.buffer_index_to_heap_index[buffer_index]),
                i
            );
        }
    }

    pub fn check(&self) {
        if cfg!(debug_assertions) {
            self.check_force();
        }
    }

    pub fn is_empty(&self) -> bool {
        self.heap_index_to_buffer_index.is_empty()
    }

    pub fn peek_min(&self) -> Option<(u16, NonZeroU16)> {
        if !self.heap_index_to_buffer_index.is_empty() {
            let buffer_index = self.heap_index_to_buffer_index[0];
            let weight = self.buffer_index_to_weight[usize::from(buffer_index)].unwrap();

            Some((buffer_index, weight))
        } else {
            None
        }
    }

    fn heap_index_to_weight(&self, heap_index: usize) -> NonZeroU16 {
        let buffer_index = usize::from(self.heap_index_to_buffer_index[heap_index]);

        self.buffer_index_to_weight[buffer_index].unwrap()
    }

    fn swap(&mut self, heap_index_a: usize, heap_index_b: usize) {
        let buffer_index_a = usize::from(self.heap_index_to_buffer_index[heap_index_a]);
        let buffer_index_b = usize::from(self.heap_index_to_buffer_index[heap_index_b]);

        self.heap_index_to_buffer_index
            .swap(heap_index_a, heap_index_b);
        self.buffer_index_to_heap_index
            .swap(buffer_index_a, buffer_index_b);
    }

    fn pull_up(&mut self, mut heap_index: usize) {
        while heap_index != 0 {
            let parent_heap_index = (heap_index - 1) / 2;

            if self.heap_index_to_weight(parent_heap_index) <= self.heap_index_to_weight(heap_index)
            {
                break;
            }

            self.swap(heap_index, parent_heap_index);
            heap_index = parent_heap_index;
        }
    }

    pub fn insert_buffer_weight(&mut self, buffer_index: usize, weight: NonZeroU16) {
        let heap_index = self.heap_index_to_buffer_index.len();
        self.heap_index_to_buffer_index
            .push(buffer_index.try_into().unwrap());

        if self.buffer_index_to_weight.len() < buffer_index + 1 {
            self.buffer_index_to_weight.resize(buffer_index + 1, None);
            self.buffer_index_to_heap_index.resize(buffer_index + 1, 0);
        }

        assert_eq!(
            replace(&mut self.buffer_index_to_weight[buffer_index], Some(weight)),
            None
        );
        self.buffer_index_to_heap_index[buffer_index] = heap_index.try_into().unwrap();

        self.pull_up(heap_index);

        self.check();
    }

    fn push_down(&mut self, mut heap_index: usize) {
        loop {
            let mut heap_index_min = heap_index;

            let child_heap_index = 2 * heap_index + 1;
            if child_heap_index < self.heap_index_to_buffer_index.len()
                && self.heap_index_to_weight(child_heap_index)
                    < self.heap_index_to_weight(heap_index_min)
            {
                heap_index_min = child_heap_index;
            }

            let child_heap_index = 2 * heap_index + 2;
            if child_heap_index < self.heap_index_to_buffer_index.len()
                && self.heap_index_to_weight(child_heap_index)
                    < self.heap_index_to_weight(heap_index_min)
            {
                heap_index_min = child_heap_index;
            }

            if heap_index == heap_index_min {
                break;
            }

            self.swap(heap_index, heap_index_min);
            heap_index = heap_index_min;
        }
    }

    fn remove_heap_index(&mut self, heap_index: u16, buffer_index: u16) {
        let heap_index_usize = usize::from(heap_index);
        let buffer_index_usize = usize::from(buffer_index);

        debug_assert_eq!(
            self.heap_index_to_buffer_index[heap_index_usize],
            buffer_index
        );
        debug_assert_eq!(
            self.buffer_index_to_heap_index[buffer_index_usize],
            heap_index
        );

        let last_heap_index = self.heap_index_to_buffer_index.len() - 1;
        if heap_index_usize != last_heap_index {
            self.swap(heap_index_usize, last_heap_index);
        }
        self.heap_index_to_buffer_index.pop();

        let prev_weight = self.buffer_index_to_weight[buffer_index_usize]
            .take()
            .unwrap()
            .get();

        if heap_index_usize != last_heap_index {
            match self
                .heap_index_to_weight(heap_index_usize)
                .get()
                .cmp(&prev_weight)
            {
                Ordering::Less => self.pull_up(heap_index_usize),
                Ordering::Greater => self.push_down(heap_index_usize),
                Ordering::Equal => {}
            }
        }

        self.check();
    }

    pub fn remove_min(&mut self) {
        self.remove_heap_index(0, self.heap_index_to_buffer_index[0]);
    }

    pub fn remove_buffer_weight(&mut self, buffer_index: usize) -> Option<NonZeroU16> {
        let weight = self.buffer_index_to_weight[buffer_index].unwrap();

        self.remove_heap_index(
            self.buffer_index_to_heap_index[buffer_index],
            buffer_index.try_into().unwrap(),
        );

        Some(weight)
    }

    pub fn update_buffer_weight(&mut self, buffer_index: usize, weight: NonZeroU16) {
        let prev_weight = self.buffer_index_to_weight[buffer_index].unwrap();

        if prev_weight != weight {
            self.buffer_index_to_weight[buffer_index] = Some(weight);

            let heap_index = usize::from(self.buffer_index_to_heap_index[buffer_index]);

            match weight.cmp(&prev_weight) {
                Ordering::Less => self.pull_up(heap_index),
                Ordering::Greater => self.push_down(heap_index),
                Ordering::Equal => {}
            }

            self.check();
        }
    }

    pub fn enumerate(&self, mut buffer_index_weight_fn: impl FnMut(u16, NonZeroU16)) {
        for buffer_index in &self.heap_index_to_buffer_index {
            let weight = self.buffer_index_to_weight[usize::from(*buffer_index)].unwrap();

            buffer_index_weight_fn(*buffer_index, weight);
        }
    }
}
