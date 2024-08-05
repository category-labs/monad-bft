use std::{cmp::Ordering, mem::replace, num::NonZeroU16};

#[derive(Debug)]
pub struct BufferWeightMap<const MAX_BUFFERS: usize> {
    // weight 1..=8419
    buffer_index_to_weight: Box<[Option<NonZeroU16>; MAX_BUFFERS]>,

    num_heap_entries: usize,
    heap_index_to_buffer_index: Box<[u16; MAX_BUFFERS]>,
    buffer_index_to_heap_index: Box<[u16; MAX_BUFFERS]>,
}

impl<const MAX_BUFFERS: usize> BufferWeightMap<MAX_BUFFERS> {
    pub fn new() -> BufferWeightMap<MAX_BUFFERS> {
        BufferWeightMap {
            buffer_index_to_weight: Box::new([None; MAX_BUFFERS]),
            num_heap_entries: 0,
            heap_index_to_buffer_index: Box::new([0; MAX_BUFFERS]),
            buffer_index_to_heap_index: Box::new([0; MAX_BUFFERS]),
        }
    }

    fn print_node(&self, heap_index: usize, indent: usize) {
        if heap_index < self.num_heap_entries {
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
        let mut buffer_index_to_weight = [None; MAX_BUFFERS];

        for i in 0..self.num_heap_entries {
            let buffer_index = usize::from(self.heap_index_to_buffer_index[i]);

            buffer_index_to_weight[buffer_index] = self.buffer_index_to_weight[buffer_index];
        }

        assert_eq!(*self.buffer_index_to_weight, buffer_index_to_weight);

        for i in 0..self.num_heap_entries {
            let buffer_index = usize::from(self.heap_index_to_buffer_index[i]);

            assert_eq!(
                usize::from(self.buffer_index_to_heap_index[buffer_index]),
                i
            );
        }

        for i in 0..self.num_heap_entries {
            let weight = self.buffer_index_to_weight
                [usize::from(self.heap_index_to_buffer_index[i])]
            .unwrap();

            let child = 2 * i + 1;
            if child < self.num_heap_entries {
                let child_weight = self.buffer_index_to_weight
                    [usize::from(self.heap_index_to_buffer_index[child])]
                .unwrap();
                assert!(weight <= child_weight);
            }

            let child = 2 * i + 2;
            if child < self.num_heap_entries {
                let child_weight = self.buffer_index_to_weight
                    [usize::from(self.heap_index_to_buffer_index[child])]
                .unwrap();
                assert!(weight <= child_weight);
            }
        }
    }

    pub fn check(&self) {
        if cfg!(debug_assertions) {
            self.check_force();
        }
    }

    pub fn is_empty(&self) -> bool {
        self.num_heap_entries == 0
    }

    pub fn peek_min(&self) -> Option<(u16, NonZeroU16)> {
        if self.num_heap_entries > 0 {
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
        assert!(replace(&mut self.buffer_index_to_weight[buffer_index], Some(weight)).is_none());

        let heap_index = self.num_heap_entries;
        self.num_heap_entries += 1;

        self.heap_index_to_buffer_index[heap_index] = buffer_index.try_into().unwrap();
        self.buffer_index_to_heap_index[buffer_index] = heap_index.try_into().unwrap();

        self.pull_up(heap_index);

        self.check();
    }

    fn push_down(&mut self, mut heap_index: usize) {
        loop {
            let mut heap_index_min = heap_index;

            let child_heap_index = 2 * heap_index + 1;
            if child_heap_index < self.num_heap_entries
                && self.heap_index_to_weight(child_heap_index)
                    < self.heap_index_to_weight(heap_index_min)
            {
                heap_index_min = child_heap_index;
            }

            let child_heap_index = 2 * heap_index + 2;
            if child_heap_index < self.num_heap_entries
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

        debug_assert!(self.heap_index_to_buffer_index[heap_index_usize] == buffer_index);
        debug_assert!(self.buffer_index_to_heap_index[buffer_index_usize] == heap_index);

        let last_heap_index = self.num_heap_entries - 1;
        if heap_index_usize != last_heap_index {
            self.swap(heap_index_usize, last_heap_index);
        }

        let prev_weight = self.buffer_index_to_weight[buffer_index_usize]
            .take()
            .unwrap()
            .get();

        self.num_heap_entries -= 1;

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

    /*
    pub fn update_buffer_weight_fn(
        &mut self,
        buffer_index: usize,
        update_fn: impl FnMut(Option<NonZeroU16>) -> Option<NonZeroU16>,
    ) {
        todo!()
    }
    */

    pub fn enumerate(&self, mut buffer_index_weight_fn: impl FnMut(u16, NonZeroU16)) {
        for i in 0..self.num_heap_entries {
            let buffer_index = self.heap_index_to_buffer_index[i];
            let weight = self.buffer_index_to_weight[usize::from(buffer_index)].unwrap();

            buffer_index_weight_fn(buffer_index, weight);
        }
    }
}

impl<const MAX_BUFFERS: usize> Default for BufferWeightMap<MAX_BUFFERS> {
    fn default() -> Self {
        Self::new()
    }
}
