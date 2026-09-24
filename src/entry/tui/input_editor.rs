use unicode_segmentation::UnicodeSegmentation;
use unicode_width::UnicodeWidthStr;

/// 面向终端输入框的字素编辑器。光标始终位于 Unicode 字素簇边界，
/// 因此删除或移动不会拆开组合字符或 ZWJ emoji。
#[derive(Default)]
pub(super) struct InputEditor {
    buffer: String,
    /// UTF-8 字节偏移，但始终由字素边界移动函数维护。
    cursor: usize,
}

impl InputEditor {
    pub(super) fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    pub(super) fn is_blank(&self) -> bool {
        self.buffer.chars().all(char::is_whitespace)
    }

    pub(super) fn is_single_line(&self) -> bool {
        !self.buffer.contains('\n')
    }

    pub(super) fn text(&self) -> String {
        self.buffer.clone()
    }

    pub(super) fn clear(&mut self) {
        self.buffer.clear();
        self.cursor = 0;
    }

    pub(super) fn replace(&mut self, text: impl AsRef<str>) {
        self.buffer = text.as_ref().to_owned();
        self.cursor = self.buffer.len();
    }

    pub(super) fn take(&mut self) -> String {
        let text = self.text();
        self.clear();
        text
    }

    pub(super) fn insert(&mut self, character: char) {
        self.buffer.insert(self.cursor, character);
        self.cursor += character.len_utf8();
        self.snap_cursor_forward();
    }

    pub(super) fn insert_text(&mut self, text: &str) {
        let text = text.replace('\r', "");
        self.buffer.insert_str(self.cursor, &text);
        self.cursor += text.len();
        self.snap_cursor_forward();
    }

    pub(super) fn backspace(&mut self) {
        if let Some(start) = self.previous_grapheme_start() {
            self.buffer.drain(start..self.cursor);
            self.cursor = start;
        }
    }

    pub(super) fn delete(&mut self) {
        if let Some(end) = self.next_grapheme_end() {
            self.buffer.drain(self.cursor..end);
        }
    }

    pub(super) fn move_left(&mut self) {
        if let Some(start) = self.previous_grapheme_start() {
            self.cursor = start;
        }
    }

    pub(super) fn move_right(&mut self) {
        if let Some(end) = self.next_grapheme_end() {
            self.cursor = end;
        }
    }

    pub(super) fn move_line_start(&mut self) {
        self.cursor = self.buffer[..self.cursor]
            .rfind('\n')
            .map_or(0, |index| index + 1);
    }

    pub(super) fn move_line_end(&mut self) {
        self.cursor = self.buffer[self.cursor..]
            .find('\n')
            .map_or(self.buffer.len(), |index| self.cursor + index);
    }

    pub(super) fn move_word_left(&mut self) {
        while self.cursor > 0 && self.previous_grapheme_is_whitespace() {
            self.move_left();
        }
        while self.cursor > 0 && !self.previous_grapheme_is_whitespace() {
            self.move_left();
        }
    }

    pub(super) fn move_word_right(&mut self) {
        while self.cursor < self.buffer.len() && self.next_grapheme_is_whitespace() {
            self.move_right();
        }
        while self.cursor < self.buffer.len() && !self.next_grapheme_is_whitespace() {
            self.move_right();
        }
    }

    pub(super) fn delete_word_left(&mut self) {
        let end = self.cursor;
        self.move_word_left();
        self.buffer.drain(self.cursor..end);
    }

    /// 返回在给定显示宽度下，光标所在的视觉行和列。宽字符按终端单元格计数。
    pub(super) fn visual_cursor(&self, width: u16) -> (usize, usize) {
        let width = usize::from(width.max(1));
        let mut row = 0;
        let mut column = 0;
        for grapheme in self.buffer[..self.cursor].graphemes(true) {
            if grapheme == "\n" {
                row += 1;
                column = 0;
                continue;
            }
            let grapheme_width = grapheme.width();
            if grapheme_width > 0 && column + grapheme_width > width {
                row += 1;
                column = 0;
            }
            column += grapheme_width;
            if column >= width {
                row += 1;
                column = 0;
            }
        }
        (row, column)
    }

    fn previous_grapheme_start(&self) -> Option<usize> {
        self.buffer[..self.cursor]
            .grapheme_indices(true)
            .next_back()
            .map(|(index, _)| index)
    }

    fn next_grapheme_end(&self) -> Option<usize> {
        self.buffer[self.cursor..]
            .graphemes(true)
            .next()
            .map(|grapheme| self.cursor + grapheme.len())
    }

    fn previous_grapheme_is_whitespace(&self) -> bool {
        self.buffer[..self.cursor]
            .graphemes(true)
            .next_back()
            .is_some_and(|grapheme| grapheme.chars().all(char::is_whitespace))
    }

    fn next_grapheme_is_whitespace(&self) -> bool {
        self.buffer[self.cursor..]
            .graphemes(true)
            .next()
            .is_some_and(|grapheme| grapheme.chars().all(char::is_whitespace))
    }

    /// 插入 ZWJ 或组合字符可能把光标两侧合并为一个新字素；向前吸附，
    /// 确保后续切片、移动与删除始终从合法字素边界开始。
    fn snap_cursor_forward(&mut self) {
        if self.cursor == self.buffer.len() {
            return;
        }
        let target = self.cursor;
        self.cursor = self
            .buffer
            .grapheme_indices(true)
            .map(|(index, _)| index)
            .chain(std::iter::once(self.buffer.len()))
            .find(|index| *index >= target)
            .unwrap_or(self.buffer.len());
    }
}

#[cfg(test)]
mod tests {
    use super::InputEditor;

    #[test]
    fn edits_cjk_without_using_byte_offsets() {
        let mut editor = InputEditor::default();
        editor.insert_text("你好 world");
        editor.delete_word_left();
        assert_eq!(editor.text(), "你好 ");
        editor.backspace();
        assert_eq!(editor.text(), "你好");
        editor.move_left();
        editor.backspace();
        assert_eq!(editor.text(), "好");
    }

    #[test]
    fn computes_wrapped_visual_cursor_for_wide_characters() {
        let mut editor = InputEditor::default();
        editor.insert_text("ab中文");
        assert_eq!(editor.visual_cursor(4), (1, 2));
        editor.insert('\n');
        editor.insert_text("x");
        assert_eq!(editor.visual_cursor(4), (2, 1));
    }

    #[test]
    fn deletes_combining_characters_and_zwj_emoji_as_graphemes() {
        let mut editor = InputEditor::default();
        editor.insert_text("a e\u{301} 👨‍👩‍👧‍👦");

        editor.backspace();
        assert_eq!(editor.text(), "a e\u{301} ");
        editor.backspace();
        editor.backspace();

        assert_eq!(editor.text(), "a ");
    }

    #[test]
    fn insertion_that_joins_neighboring_emoji_keeps_cursor_on_a_grapheme_boundary() {
        let mut editor = InputEditor::default();
        editor.insert_text("👨👩");
        editor.move_left();
        editor.insert('\u{200d}');

        editor.backspace();
        assert_eq!(editor.text(), "");
    }
}
