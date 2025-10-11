use similar::TextDiff;
use std::iter::{Enumerate, Peekable};
use unicode_segmentation::{Graphemes, UnicodeSegmentation};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TextChange {
    Insert { at: usize, value: String },
    Delete { at: usize, len: usize },
}

pub fn diff(from: &str, to: &str) -> Vec<TextChange> {
    let diff = TextDiff::from_graphemes(from, to);
    // debug_helpers::print_diff_changes(&diff);
    let ops = diff.ops(); //iff.grouped_ops(1);
    // eprintln!("ops:\n{ops:#?}");
    // debug_helpers::print_ops_changes(from, to, ops);

    let changes: Vec<TextChange> = {
        let mut builder = Vec::with_capacity(ops.len());
        let mut copy_to_index = 0usize;
        for change in ops.iter() {
            match change {
                similar::DiffOp::Equal { old_index, len, .. } => {
                    copy_to_index = old_index + len;
                    // Skip equal text.
                }
                similar::DiffOp::Delete {
                    old_index, old_len, ..
                } => {
                    builder.push(TextChange::Delete {
                        at: *old_index,
                        len: *old_len,
                    });
                }
                similar::DiffOp::Insert {
                    new_index, new_len, ..
                } => {
                    let mut to_cursor = GraphemeCursor::new(to);
                    let mut new_text = String::new();
                    to_cursor.skip(*new_index);
                    to_cursor.copy_to_until(&mut new_text, new_index + new_len);
                    builder.push(TextChange::Insert {
                        at: copy_to_index,
                        value: new_text,
                    });
                }
                similar::DiffOp::Replace {
                    old_index,
                    old_len,
                    new_index,
                    new_len,
                } => {
                    let mut to_cursor = GraphemeCursor::new(to);
                    let mut new_text = String::new();
                    to_cursor.skip(*new_index);
                    to_cursor.copy_to_until(&mut new_text, new_index + new_len);
                    builder.push(TextChange::Delete {
                        at: *old_index,
                        len: *old_len,
                    });
                    builder.push(TextChange::Insert {
                        at: *old_index + *old_len,
                        value: new_text,
                    });
                }
            }
        }

        // Before we return this, let's double check it really satisfied our requirements.
        let mut last_pos = 0usize;
        for change in builder.iter() {
            match change {
                TextChange::Insert { at, .. } => {
                    assert!(last_pos <= *at, "The list of changes was misordered.");
                    last_pos = *at;
                }
                TextChange::Delete { at, len } => {
                    assert!(last_pos <= *at, "The list of changes was misordered.");
                    last_pos = at + len;
                }
            }
        }

        builder
    };

    changes
}

// fn temp_debug_show_cursor_pos(cursor: &mut GraphemeCursor, text: &str) {
//     let current_cursor_pos = cursor.position().unwrap();
//     let mut fresh_cursor = GraphemeCursor::new(text);
//     let mut text_with_pos = String::new();
//     fresh_cursor.copy_to_until(&mut text_with_pos, current_cursor_pos);
//     text_with_pos.push_str(&format!("\x1b[7m{}\x1b[0m", fresh_cursor.next().unwrap()));
//     fresh_cursor.copy_to(&mut text_with_pos);
//     println!("Cursor position ({current_cursor_pos}):'\n{text_with_pos}\n'")
// }

pub fn apply_text_diff(text: &str, diff: &[TextChange]) -> String {
    // Assuming that we'll need roughly the same as the input in size seems like a fair bet.
    let mut output = String::with_capacity(text.len());
    let mut input_cursor = GraphemeCursor::new(text);

    for change in diff {
        // println!("Applying change: {change:?}");
        // temp_debug_show_cursor_pos(&mut input_cursor, text);
        match change {
            TextChange::Insert { at, value } => {
                input_cursor.copy_to_until(&mut output, *at);
                // temp_debug_show_cursor_pos(&mut input_cursor, text);
                output.push_str(value);
            }
            TextChange::Delete { at, len } => {
                input_cursor.copy_to_until(&mut output, *at);
                // temp_debug_show_cursor_pos(&mut input_cursor, text);
                input_cursor.skip(*len);
            }
        }
        // println!("Output is now: '\n{output}\n'");
    }
    input_cursor.copy_to(&mut output);

    output
}

struct GraphemeCursor<'s> {
    iter: Peekable<Enumerate<Graphemes<'s>>>,
}
impl<'s> GraphemeCursor<'s> {
    pub fn new(text: &'s str) -> Self {
        Self {
            iter: text.graphemes(true).enumerate().peekable(),
        }
    }

    #[allow(unused)]
    pub fn position(&mut self) -> Option<usize> {
        self.iter.peek().map(|(index, _)| *index)
    }

    #[allow(unused)]
    pub fn next(&mut self) -> Option<&'s str> {
        self.iter.next().map(|(_, s)| s)
    }

    pub fn skip(&mut self, len: usize) {
        for _i in 0..len {
            self.iter.next().expect("Skipped beyond end of cursor"); // Ignore these.
        }
    }

    pub fn copy_to_until(&mut self, target: &mut String, until_pos: usize) {
        while self
            .iter
            .peek()
            .filter(|(index, _)| *index < until_pos)
            .is_some()
        {
            let (_, next_grapheme) = self.iter.next().expect("Copied beyond end of cursor");
            target.push_str(next_grapheme);
        }
    }

    pub fn copy_to(self, target: &mut String) {
        for (_, next_grapheme) in self.iter {
            target.push_str(next_grapheme);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use itertools::Itertools;

    // #[test]
    // fn simple_test() {
    //     let from = "The fox is rather silly.";
    //     let to = "The fox is now quite serious!";
    //     let res = diff(from, to);
    //     assert_ne!(res, vec![]);
    //     let applied = apply_text_diff(from, &res);
    //     assert_eq!(applied, to);
    // }

    // #[test]
    // fn grapheme_test() {
    //     let from = "The 🇸🇪 fox is rather silly.";
    //     let to = "The fox is now a quite serious 🇩🇪! It likes 🌼, though.";
    //     let res = diff(from, to);
    //     assert_ne!(res, vec![]);
    //     let applied = apply_text_diff(from, &res);
    //     assert_eq!(applied, to);
    // }

    // 30 strings grouped into 10 triplets of small diffs.
    // Covers: empty, ASCII edits, combining marks, emoji + skin tone, ZWJ sequences,
    // NBSP vs spaces, line endings, zero-width chars, tabs/trailing.
    const SMALL_CHANGE_TEST_GROUPS: [[&str; 3]; 10] = [
        // Empty → small growth
        ["", "a", "ab"],
        // Word changes.
        [
            "The quick brown fox jumps over the lazy dog.",
            "The small brown fox hops over the lazy dog.",
            "The small brown fox hops quickly over the irritated camel.",
        ],
        // ASCII punctuation + trailing space
        [
            "The quick brown fox jumps over the lazy dog.",
            "The quick brown fox jumps over the lazy dog!",
            "The quick brown fox jumps over the lazy dog! ",
        ],
        // Combining vs precomposed vs plain
        ["cafe\u{301}", "café", "cafe"],
        // Emoji with skin tone + tiny word change
        ["👍 great job", "👍🏽 great job", "👍🏽 great jobs"],
        // ZWJ family sequence shortened stepwise
        [
            "👨\u{200D}👩\u{200D}👧\u{200D}👦",
            "👨\u{200D}👩\u{200D}👧",
            "👨\u{200D}👩",
        ],
        // NBSP vs normal/double space
        [
            "The quick brown fox",
            "The quick\u{00A0}brown fox",
            "The quick  brown fox",
        ],
        // Line endings
        ["Line", "Line\n", "Line\r\n"],
        // Zero-width joiner/space differences
        ["zerowidth", "zero\u{200B}width", "zero\u{200D}width"],
        // Tabs and trailing tab
        [
            "\tIndented line",
            "\tIndented line\t",
            "\tIndented line\twith\ttabs",
        ],
    ];

    #[test]
    fn diff_and_apply_small_changes() {
        // Do all possible transitions within each group.
        for (row, group) in SMALL_CHANGE_TEST_GROUPS.iter().enumerate() {
            for perm in group.iter().enumerate().permutations(2) {
                let (from_index, from) = perm[0];
                let (to_index, to) = perm[1];
                check_diff_and_apply(
                    from,
                    to,
                    &format!(
                        "Patching with input:\n    {row}:{from_index}: \"{from}\"\n -> {row}:{to_index}: \"{to}\""
                    ),
                );
            }
        }
    }

    #[test]
    fn diff_and_apply_distant_changes() {
        // Do some changes across groups.
        for perm in SMALL_CHANGE_TEST_GROUPS.iter().enumerate().permutations(2) {
            let (from_row, from_group) = perm[0];
            let (to_row, to_group) = perm[1];
            // I don't feel it's worth testing all possible combinations here. I'll just go from the initial value of one group to the final of another.
            let from_index = 0;
            let to_index = 2;
            let from = from_group[from_index];
            let to = to_group[to_index];
            check_diff_and_apply(
                from,
                to,
                &format!(
                    "Patching with input:\n    {from_row}:{from_index}: \"{from}\"\n -> {to_row}:{to_index}: \"{to}\""
                ),
            );
        }
    }

    const TEXT_A: &str = r#"
# Harbor Log — Entry Ⅰ

The **ancient** harbor lay silent beneath the pale morning light.  
Gulls 🕊 circling lazily above crooked masts—occasionally diving toward the dark water.  
Fishermen once gathered here at dawn:

- Laughing as they prepared their nets 🎣  
- Sharing bread and coffee ☕  
- Cursing the cold wind 🌬

Now, only echoes of their voices remained.  
A thin mist clung to the worn stone pier, wrapping everything in a cold embrace.  
Somewhere in the distance, a *bell* tolled with a hollow, metallic ring 🔔.  
No ships had docked here in years, yet the smell of salt and tar lingered stubbornly.  
It felt as though the sea itself refused to forget. 🌊⚓
"#;

    const TEXT_B: &str = r#"
# Harbour Log — Entry II

The **old** harbour stood quiet under the dim morning glow.  
Seagulls 🕊 drifting in slow circles above leaning masts—sometimes swooping toward the black waters.  
Sailors once gathered here at first light:

- Smiling while they checked their gear 🧰  
- Tearing bread and sipping bitter tea 🍵  
- Grumbling at the biting wind 🌪

Now, only faint traces of their laughter survive.  
A light fog clung to the cracked stone jetty, coating everything in a damp shroud.  
Far in the distance, a *bell* chimed with a hollow, echoing tone 🔔.  
No boats had berthed here in ages, yet the scent of salt 🧂 and pitch persisted defiantly.  
It seemed as though the ocean itself refused to let go. 🌊⚓🪝
"#;
    #[test]
    fn diff_and_apply_larger_changes() {
        check_diff_and_apply(
            TEXT_A,
            TEXT_B,
            &format!("Patching with large input:\n\"\n{TEXT_A}\n\"\n ->\n\"\n{TEXT_B}\n\""),
        );

        // And reverse.
        // check_diff_and_apply(
        //     TEXT_B,
        //     TEXT_A,
        //     &format!("Patching with large input:\n\"\n{TEXT_B}\n\"\n ->\n\"\n{TEXT_A}\n\""),
        // );
    }

    #[test]
    fn debug_me_test() {
        let from = r#"
# Harbor Log — Entry Ⅰ

The **ancient** harbor lay silent beneath the pale morning light.  
Gulls 🕊 circling lazily above crooked masts—occasionally diving toward the dark water.  
Fishermen once gathered here at dawn:

- Laughing as they prepared their nets 🎣  
"#;
        let to = r#"
# Harbour Log — Entry II

The **old** harbour stood quiet under the dim morning glow.  
Seagulls drifting in slow circles above leaning masts—sometimes swooping toward the black waters.  
"#;

        check_diff_and_apply(
            from,
            to,
            &format!("Patching with large input:\n\"\n{from}\n\"\n ->\n\"\n{to}\n\""),
        );
    }

    fn check_diff_and_apply(from: &str, to: &str, error_context: &str) {
        let result = diff(from, to);
        assert_ne!(
            result,
            vec![],
            "Diff should not be empty.\n  Context: {error_context}"
        );
        let applied = apply_text_diff(from, &result);
        assert_eq!(applied, to, "{error_context}");
    }
}

#[allow(unused)]
mod debug_helpers {
    use similar::DiffOp;

    use super::*;

    pub fn print_diff_changes<'a>(diff: &TextDiff<'a, 'a, '_, str>) {
        let mut diff_str = String::new();
        for entry in diff.iter_all_changes() {
            match entry.tag() {
                similar::ChangeTag::Equal => {
                    diff_str.push_str(entry.as_str().unwrap());
                }
                similar::ChangeTag::Delete => {
                    diff_str.push_str(&format!("\x1b[31m{}\x1b[0m", entry.as_str().unwrap()));
                }
                similar::ChangeTag::Insert => {
                    diff_str.push_str(&format!("\x1b[32m{}\x1b[0m", entry.as_str().unwrap()));
                }
            }
        }
        eprintln!("diff: '{diff_str}'");
    }

    pub fn print_ops_changes(from: &str, to: &str, ops: &[DiffOp]) {
        let mut diff_str = String::new();
        // eprintln!("diff:\n----");
        for entry in ops.iter() {
            match entry {
                similar::DiffOp::Equal {
                    old_index,
                    new_index,
                    len,
                } => {
                    let mut from_cursor = GraphemeCursor::new(from);
                    let mut text = String::new();
                    from_cursor.skip(*old_index);
                    from_cursor.copy_to_until(&mut text, old_index + len);
                    diff_str.push_str(&text);
                }
                similar::DiffOp::Delete {
                    old_index,
                    old_len,
                    new_index,
                } => {
                    let mut from_cursor = GraphemeCursor::new(from);
                    let mut old_text = String::new();
                    from_cursor.skip(*old_index);
                    from_cursor.copy_to_until(&mut old_text, old_index + old_len);
                    diff_str.push_str(&format!("\x1b[31m{old_text}\x1b[0m"));
                }
                similar::DiffOp::Insert {
                    old_index,
                    new_index,
                    new_len,
                } => {
                    let mut to_cursor = GraphemeCursor::new(to);
                    let mut new_text = String::new();
                    to_cursor.skip(*new_index);
                    to_cursor.copy_to_until(&mut new_text, new_index + new_len);
                    diff_str.push_str(&format!("\x1b[32m{new_text}\x1b[0m"));
                }
                similar::DiffOp::Replace {
                    old_index,
                    old_len,
                    new_index,
                    new_len,
                } => {
                    let mut from_cursor = GraphemeCursor::new(from);
                    let mut old_text = String::new();
                    from_cursor.skip(*old_index);
                    from_cursor.copy_to_until(&mut old_text, old_index + old_len);
                    let mut to_cursor = GraphemeCursor::new(to);
                    let mut new_text = String::new();
                    to_cursor.skip(*new_index);
                    to_cursor.copy_to_until(&mut new_text, new_index + new_len);
                    diff_str.push_str(&format!("\x1b[31m{old_text}\x1b[0m"));
                    diff_str.push_str(&format!("\x1b[32m{new_text}\x1b[0m"));
                }
            }
        }
        eprintln!("ops diff: '{diff_str}'");
    }
}
