use anyhow::Error;
use std::iter::Enumerate;
use std::slice::Iter;

#[derive(PartialOrd, PartialEq)]
pub struct DechibMessage {
    pub(crate) message_type: MessageType,
    pub(crate) message_size: usize,
    pub(crate) message_content: Vec<u8>,
}

#[derive(Debug, PartialEq, PartialOrd)]
pub enum MessageType {
    Init,
    Message,
}

impl DechibMessage {
    pub fn from_message_type(
        enumerable_value: &mut Enumerate<Iter<u8>>,
        message_type: MessageType,
    ) -> Result<DechibMessage, Error> {
        if let Some((_, message_size)) = enumerable_value.next() {
            let message_size = message_size.to_owned() as usize;

            let mut message_content: Vec<u8> = enumerable_value
                .take(message_size)
                .map(|(_, val)| *val)
                .collect();

            if message_content.len() < message_size {
                message_content.resize(message_size, 0);
            }

            Ok(DechibMessage {
                message_type,
                message_size,
                message_content,
            })
        } else {
            Err(Error::msg("incoming message does not contain message_size"))
        }
    }
}

impl TryFrom<&[u8]> for DechibMessage {
    type Error = Error;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        let mut enumerable_value = value.into_iter().enumerate();
        if let Some((_, val)) = enumerable_value.next() {
            let val_char = char::try_from(val.to_owned()).unwrap_or_else(|err| panic!("{}", err));

            match val_char {
                'I' => Self::from_message_type(&mut enumerable_value, MessageType::Init),
                'M' => Self::from_message_type(&mut enumerable_value, MessageType::Message),
                _ => Err(Error::msg(
                    "message_type is not \'I\' (init_mode) or \'M\' (message_mode)",
                )),
            }
        } else {
            Err(Error::msg(
                "incoming message does not contain a message_type",
            ))
        }
    }
}

mod tests {
    use super::*;

    #[test]
    fn should_try_from() {
        let input: &[u8] = &[b'M', 5, b'h', b'e', b'l', b'l', b'o'];
        let result = DechibMessage::try_from(input).unwrap();
        let expected = DechibMessage {
            message_type: MessageType::Message,
            message_size: 5,
            message_content: vec![104, 101, 108, 108, 111],
        };

        assert_eq!(result.message_size, expected.message_size);
        assert_eq!(result.message_content, expected.message_content);
        assert_eq!(result.message_type, expected.message_type)
    }

    #[test]
    fn should_fail_try_from_wrong_msg_type() {
        let expected_error = "message_type is not \'I\' (init_mode) or \'M\' (message_mode)";
        let input: &[u8] = &[b'Z', 5, b'h', b'e', b'l', b'l', b'o'];
        match DechibMessage::try_from(input) {
            Ok(v) => {
                panic!("should not have succeeded");
            }
            Err(error) => {
                assert_eq!(error.to_string(), expected_error);
            }
        }
    }

    #[test]
    fn should_fail_try_from_wrong_size() {
        let expected_error = "incoming message does not contain message_size";
        let input: &[u8] = &[b'M'];
        match DechibMessage::try_from(input) {
            Ok(v) => {
                panic!("should not have succeeded");
            }
            Err(error) => {
                assert_eq!(error.to_string(), expected_error);
            }
        }
    }

    #[test]
    fn should_only_get_message_content_with_size() {
        let input: &[u8] = &[b'M', 5, b'h', b'e', b'l', b'l', b'o', b'f', b'o', b'o'];
        let result = DechibMessage::try_from(input).unwrap();
        let expected = DechibMessage {
            message_type: MessageType::Message,
            message_size: 5,
            // does not contain b"foo"
            message_content: vec![104, 101, 108, 108, 111],
        };

        assert_eq!(result.message_size, expected.message_size);
        assert_eq!(result.message_content, expected.message_content);
        assert_eq!(result.message_type, expected.message_type)
    }

    #[test]
    fn content_less_then_size() {
        let input: &[u8] = &[b'M', 5, b'h', b'e'];
        let result = DechibMessage::try_from(input).unwrap();
        let expected = DechibMessage {
            message_type: MessageType::Message,
            message_size: 5,
            message_content: vec![104, 101, 0, 0, 0],
        };

        assert_eq!(result.message_size, expected.message_size);
        assert_eq!(result.message_content, expected.message_content);
        assert_eq!(result.message_type, expected.message_type)
    }

    #[test]
    fn content_is_zero() {
        let input: &[u8] = &[b'M', 5];
        let result = DechibMessage::try_from(input).unwrap();
        let expected = DechibMessage {
            message_type: MessageType::Message,
            message_size: 5,
            message_content: vec![0, 0, 0, 0, 0],
        };

        assert_eq!(result.message_size, expected.message_size);
        assert_eq!(result.message_content, expected.message_content);
        assert_eq!(result.message_type, expected.message_type)
    }
}
