use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum TuiAction {
    Escape,
    ToggleHelp,
    PageUp,
    PageDown,
    ToggleToolDetails,
    ClearInput,
    ClearQueue,
    CancelTurn,
    Previous,
    Next,
    Complete,
    CursorWordLeft,
    CursorWordRight,
    CursorLeft,
    CursorRight,
    ScrollTop,
    ScrollBottom,
    ScrollLineUp,
    ScrollLineDown,
    CursorLineStart,
    CursorLineEnd,
    DeleteWordLeft,
    Backspace,
    Delete,
    InsertNewline,
    Submit,
}

#[derive(Clone, Copy)]
struct Shortcut {
    code: KeyCode,
    modifiers: KeyModifiers,
}

impl Shortcut {
    const fn new(code: KeyCode, modifiers: KeyModifiers) -> Self {
        Self { code, modifiers }
    }

    fn matches(self, event: KeyEvent) -> bool {
        let mut modifiers = event.modifiers;
        if matches!(self.code, KeyCode::Char(_)) {
            modifiers.remove(SHIFT);
        }
        self.code == event.code && self.modifiers == modifiers
    }
}

struct ActionDefinition {
    action: TuiAction,
    shortcuts: &'static [Shortcut],
    display: &'static str,
    help: Option<&'static str>,
}

const NONE: KeyModifiers = KeyModifiers::NONE;
const CTRL: KeyModifiers = KeyModifiers::CONTROL;
const ALT: KeyModifiers = KeyModifiers::ALT;
const SHIFT: KeyModifiers = KeyModifiers::SHIFT;

const DEFINITIONS: &[ActionDefinition] = &[
    ActionDefinition {
        action: TuiAction::Escape,
        shortcuts: &[Shortcut::new(KeyCode::Esc, NONE)],
        display: "Esc",
        help: Some("关闭弹层 / 退出"),
    },
    ActionDefinition {
        action: TuiAction::ToggleHelp,
        shortcuts: &[
            Shortcut::new(KeyCode::F(1), NONE),
            Shortcut::new(KeyCode::Char('/'), CTRL),
        ],
        display: "F1 / Ctrl+/",
        help: Some("打开/关闭帮助"),
    },
    ActionDefinition {
        action: TuiAction::PageUp,
        shortcuts: &[Shortcut::new(KeyCode::PageUp, NONE)],
        display: "PageUp",
        help: Some("对话/审批向上翻页"),
    },
    ActionDefinition {
        action: TuiAction::PageDown,
        shortcuts: &[Shortcut::new(KeyCode::PageDown, NONE)],
        display: "PageDown",
        help: Some("对话/审批向下翻页"),
    },
    ActionDefinition {
        action: TuiAction::ToggleToolDetails,
        shortcuts: &[Shortcut::new(KeyCode::Char('t'), CTRL)],
        display: "Ctrl+T",
        help: Some("展开/收起工具详情"),
    },
    ActionDefinition {
        action: TuiAction::ClearInput,
        shortcuts: &[Shortcut::new(KeyCode::Char('u'), CTRL)],
        display: "Ctrl+U",
        help: None,
    },
    ActionDefinition {
        action: TuiAction::ClearQueue,
        shortcuts: &[Shortcut::new(KeyCode::Char('k'), CTRL)],
        display: "Ctrl+K",
        help: Some("清空排队消息"),
    },
    ActionDefinition {
        action: TuiAction::CancelTurn,
        shortcuts: &[Shortcut::new(KeyCode::Char('c'), CTRL)],
        display: "Ctrl+C",
        help: Some("取消当前请求"),
    },
    ActionDefinition {
        action: TuiAction::ScrollTop,
        shortcuts: &[Shortcut::new(KeyCode::Home, CTRL)],
        display: "Ctrl+Home",
        help: None,
    },
    ActionDefinition {
        action: TuiAction::ScrollBottom,
        shortcuts: &[Shortcut::new(KeyCode::End, CTRL)],
        display: "Ctrl+End",
        help: Some("回到最新消息"),
    },
    ActionDefinition {
        action: TuiAction::ScrollLineUp,
        shortcuts: &[Shortcut::new(KeyCode::Up, CTRL)],
        display: "Ctrl+↑",
        help: None,
    },
    ActionDefinition {
        action: TuiAction::ScrollLineDown,
        shortcuts: &[Shortcut::new(KeyCode::Down, CTRL)],
        display: "Ctrl+↓",
        help: None,
    },
    ActionDefinition {
        action: TuiAction::CursorWordLeft,
        shortcuts: &[
            Shortcut::new(KeyCode::Left, CTRL),
            Shortcut::new(KeyCode::Left, ALT),
        ],
        display: "Ctrl/Alt+←",
        help: None,
    },
    ActionDefinition {
        action: TuiAction::CursorWordRight,
        shortcuts: &[
            Shortcut::new(KeyCode::Right, CTRL),
            Shortcut::new(KeyCode::Right, ALT),
        ],
        display: "Ctrl/Alt+→",
        help: None,
    },
    ActionDefinition {
        action: TuiAction::CursorLeft,
        shortcuts: &[Shortcut::new(KeyCode::Left, NONE)],
        display: "←",
        help: None,
    },
    ActionDefinition {
        action: TuiAction::CursorRight,
        shortcuts: &[Shortcut::new(KeyCode::Right, NONE)],
        display: "→",
        help: None,
    },
    ActionDefinition {
        action: TuiAction::Previous,
        shortcuts: &[Shortcut::new(KeyCode::Up, NONE)],
        display: "↑",
        help: Some("上一条输入 / 上一项"),
    },
    ActionDefinition {
        action: TuiAction::Next,
        shortcuts: &[Shortcut::new(KeyCode::Down, NONE)],
        display: "↓",
        help: Some("下一条输入 / 下一项"),
    },
    ActionDefinition {
        action: TuiAction::Complete,
        shortcuts: &[
            Shortcut::new(KeyCode::Tab, NONE),
            Shortcut::new(KeyCode::BackTab, NONE),
            Shortcut::new(KeyCode::BackTab, SHIFT),
        ],
        display: "Tab",
        help: Some("补全斜杠命令"),
    },
    ActionDefinition {
        action: TuiAction::CursorLineStart,
        shortcuts: &[
            Shortcut::new(KeyCode::Home, NONE),
            Shortcut::new(KeyCode::Char('a'), CTRL),
        ],
        display: "Home / Ctrl+A",
        help: None,
    },
    ActionDefinition {
        action: TuiAction::CursorLineEnd,
        shortcuts: &[
            Shortcut::new(KeyCode::End, NONE),
            Shortcut::new(KeyCode::Char('e'), CTRL),
        ],
        display: "End / Ctrl+E",
        help: None,
    },
    ActionDefinition {
        action: TuiAction::DeleteWordLeft,
        shortcuts: &[
            Shortcut::new(KeyCode::Backspace, CTRL),
            Shortcut::new(KeyCode::Backspace, ALT),
            Shortcut::new(KeyCode::Char('w'), CTRL),
        ],
        display: "Ctrl+W",
        help: None,
    },
    ActionDefinition {
        action: TuiAction::Backspace,
        shortcuts: &[Shortcut::new(KeyCode::Backspace, NONE)],
        display: "Backspace",
        help: None,
    },
    ActionDefinition {
        action: TuiAction::Delete,
        shortcuts: &[
            Shortcut::new(KeyCode::Delete, NONE),
            Shortcut::new(KeyCode::Char('d'), CTRL),
        ],
        display: "Delete / Ctrl+D",
        help: None,
    },
    ActionDefinition {
        action: TuiAction::InsertNewline,
        shortcuts: &[
            Shortcut::new(KeyCode::Enter, ALT),
            Shortcut::new(KeyCode::Char('j'), CTRL),
        ],
        display: "Alt+Enter / Ctrl+J",
        help: Some("插入换行"),
    },
    ActionDefinition {
        action: TuiAction::Submit,
        shortcuts: &[Shortcut::new(KeyCode::Enter, NONE)],
        display: "Enter",
        help: Some("发送消息 / 确认"),
    },
];

pub(super) fn resolve(event: KeyEvent) -> Option<TuiAction> {
    DEFINITIONS
        .iter()
        .find(|definition| {
            definition
                .shortcuts
                .iter()
                .any(|shortcut| shortcut.matches(event))
        })
        .map(|definition| definition.action)
}

pub(super) fn approval_decision(event: KeyEvent) -> Option<bool> {
    if event.modifiers.intersects(CTRL | ALT | KeyModifiers::SUPER) {
        return None;
    }
    match event.code {
        KeyCode::Char('y' | 'Y') => Some(true),
        KeyCode::Char('n' | 'N') | KeyCode::Enter => Some(false),
        _ => None,
    }
}

pub(super) fn help_entries() -> impl Iterator<Item = (&'static str, &'static str)> {
    DEFINITIONS
        .iter()
        .filter_map(|definition| definition.help.map(|help| (definition.display, help)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolves_default_and_pi_style_editor_aliases() {
        assert_eq!(
            resolve(KeyEvent::new(KeyCode::Char('t'), CTRL)),
            Some(TuiAction::ToggleToolDetails)
        );
        assert_eq!(
            resolve(KeyEvent::new(KeyCode::Left, ALT)),
            Some(TuiAction::CursorWordLeft)
        );
        assert_eq!(
            resolve(KeyEvent::new(KeyCode::Char('j'), CTRL)),
            Some(TuiAction::InsertNewline)
        );
        assert_eq!(
            resolve(KeyEvent::new(KeyCode::Char('/'), CTRL | SHIFT)),
            Some(TuiAction::ToggleHelp)
        );
    }

    #[test]
    fn help_is_generated_from_action_definitions() {
        let help = help_entries().collect::<Vec<_>>();
        assert!(help.contains(&("Ctrl+C", "取消当前请求")));
        assert!(help.contains(&("Enter", "发送消息 / 确认")));
        assert!(help.contains(&("Tab", "补全斜杠命令")));
        assert_eq!(help.iter().filter(|(keys, _)| *keys == "Ctrl+T").count(), 1);
    }
}
