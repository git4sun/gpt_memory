# GPT Memory

GPT Memory is a package that stores historical memory of your LLM conversations and uses these memories to improve subsequent interactions. The system automatically classifies the memories and generates abstracts to behave like how human making conversations.

## Features

- **Historical Memory Storage**: Stores and manages historical chat data.
- **Automatic Classification**: Automatically classifies chat memories for better organization.
- **Abstract Generation**: Generates abstracts of past conversations to enhance GPT's context handling.
- **Mood detection: Check user emotional status and store in history chat data.
- **Context generation: Generates context for current message based on historical conversation.

## Installation

To install GPT Memory, use pip:

```bash
pip install gpt_memory
```

## Usage

### Importing and Using the Memory Class

To start using GPT Memory, import the `Memory` class and process a user message:

```python
from gpt_memory import Memory

m = Memory()
status, response = m.process_message('user_message')
```

### Command Line Chat Interface

You can also run the command line chat interface to interact with GPT Memory:

```bash
python ui.py
```

## Example

Here’s a simple example of how to use the `Memory` class:

```python
from gpt_memory import Memory

# Initialize the memory system
memory_system = Memory()

# Process a user message
status, response = memory_system.process_message('Hello, how are you?')

# Print the status and response
print(f"Status: {status}")
print(f"Response: {response}")
```

## Contributing

If you would like to contribute to GPT Memory, please follow these steps:

1. Fork the repository.
2. Create a new branch for your feature or bugfix.
3. Implement your changes.
4. Submit a pull request.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

## Contact

For any questions or inquiries, please contact [your.email@example.com](mailto:your.email@example.com).
