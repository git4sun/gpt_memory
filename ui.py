from gpt_memory.memory import Memory
import re
def user_interaction():
    m = Memory()
    print("Welcome to the chat! Type your message and hit enter. Type 'quit' or 'end' to stop.")
    
    feedback_mode = False  # Flag to track whether the UI is in feedback mode
    feedback_msg = ''
    try:
        while True:
            user_message = input("You: ")
            if user_message.lower() in ['quit', 'end']:
                print("Ending the conversation. Goodbye!")
                break
            
            if user_message.lower() == 'fb':
                # Record the feedback in the database and revert to normal chat function
                print('What do you want to say? \n', user_message)
                m.record_feedback(user_message)  # Assume this method exists to handle feedback storage
                continue
            
            if feedback_mode:
                print(feedback_msg,':\n', user_message)
                m.record_feedback(user_message)  # Assume this method exists to handle feedback storage
                continue

            if user_message.lower() == 'show db':
                h = m.show_mem()
                print(f"{'MID':<5} {'ts':<20} {'role':<8} {'continued':<10} {'Text':<50}")
                for k in h:
                    print(f"{k:<5} {h[k]['ts']:<20} {h[k]['role']:<8} {h[k]['continued']:<10} {h[k]['text']:<50}")
                continue

            if user_message.lower().startswith('delete msg'):
                ts = user_message.lower().split('msg')
                if len(ts) >1:
                    ids = [int(i) for i in re.split(r"[,;\s]\s*", ts[1]) if i]
                    m.delete_mem(ids)
                continue

            # Process the message and handle based on status code
            status, response = m.process_message(user_message)
            print("System:", response)

            if status == 2:  # Enter feedback mode
                feedback_mode = True
                feedback_msg = response

    except EOFError:  # Handles Ctrl-D
        print("\nCtrl-D pressed. Ending the conversation. Goodbye!")
    except KeyboardInterrupt:  # Handles Ctrl-C
        print("\nConversation interrupted by user. Exiting...")

if __name__ == "__main__":
    user_interaction()
