import streamlit as st
import requests
import pandas as pd
import os
from dotenv import load_dotenv
import streamlit as st
import openai

load_dotenv()
BASE_URL = os.getenv("BASE_URL")

def show_chatbot(token):
    
# #     # Initialize session state for conversation history
# #     if 'conversation_history' not in st.session_state:
# #         st.session_state['conversation_history'] = []

# #     # Function to process the query and get response
# #     def get_query_embedding(query, history, token):
# #         headers = {"Authorization": f"Bearer {token}"}
# #         response = requests.post(f"{BASE_URL}/embeddings", json={"text": query}, headers=headers)
# #         if response.status_code == 200:
# #             return response.json()
# #         else:
# #             st.error(f"Failed to fetch data: {response.status_code} - {response.text}")
# #             return None

# #     # Function to create DataFrame for display
# #     def create_display_df(business_details):
# #         df = pd.DataFrame({
# #             "Image": [details["image_url"] for details in business_details],
# #             "Name": [details["name"] for details in business_details],
# #             "Rating": [details["rating"] for details in business_details],
# #             "Review Count": [details["review_count"] for details in business_details]
# #         })
# #         return df

# #     st.title("Review Query Chatbot")

# #     # Chat interface
# #     with st.form("chat_form"):
# #         user_input = st.text_input("Ask a question about reviews:")
# #         submit_button = st.form_submit_button("Send")

# #     # Process input when the form is submitted
# #     if submit_button and user_input:
# #         # Insert user query at the beginning of conversation history
# #         st.session_state['conversation_history'].insert(0, {"role": "user", "content": user_input})

# #         # Process the query
# #         response = get_query_embedding(user_input, st.session_state['conversation_history'],token)

# #         # Determine the bot response and update conversation history
# #         if 'error' in response:
# #             bot_response = f"Error: {response['error']}"
# #         elif 'business_details' in response:
# #             business_details = response['business_details']
# #             df = create_display_df(business_details)
            
# #             # Display DataFrame
# #             st.dataframe(df[['Name', 'Rating', 'Review Count']])

# #             # Interactive feature to display more details
# #             for idx, details in enumerate(business_details):
# #                 if st.button(f"More Details", key=idx):
# #                     # Display more details for the selected business
# # #                     st.image(details["image_url"], width=300)
# # #                     st.write(f"**Name:** {details['name']}")
# # #                     st.write(f"**Rating:** {details['rating']} - **Review Count:** {details['review_count']}")
# # #                     st.write(f"**Categories:** {', '.join(details['categories'])}")
# # #                     st.write(f"**Address:** {details['display_address']}")
# # #                     st.write(f"**Price Range:** {details.get('price', 'N/A')}")
# # #                     st.write(f"**Phone:** {details['phone']}")
# # #                     st.write(f"**URL:** [Yelp Page]({details['url']})")
# # #         else:
# # #             bot_response = "No matches found or unexpected response format."

# # #         # Insert bot response at the beginning of conversation history
# # #         st.session_state['conversation_history'].insert(0, {"role": "bot", "content": bot_response})

# # #     # Display conversation history with the most recent messages first
# # #     for message in st.session_state['conversation_history']:
# # #         with st.expander(f"{message['role'].title()} says:", expanded=True):
# # #             st.write(message["content"])

# # def get_business_details(query, token):
# #     headers = {"Authorization": f"Bearer {token}"}
# #     response = requests.post(f"{BASE_URL}/embeddings", json={"query": query}, headers=headers)
# #     if response.status_code == 200:
# #         return response.json().get('business_details', [])
# #     else:
# #         st.error(f"Failed to fetch business details: {response.status_code} - {response.text}")
# #         return []

# # def create_display_df(business_details):
# #     df = pd.DataFrame(business_details)
# #     return df

# # def show_chatbot(token):
# #     st.title("Business Review Query Chatbot")
    
# #     user_input = st.text_input("Enter your query about businesses:")
# #     if st.button("Search"):
# #         # Get business details based on the query
# #         business_details = get_business_details(user_input, token)

# #         if business_details:
# #             # Display DataFrame
# #             df = create_display_df(business_details)
# #             st.dataframe(df)
# #         else:
# #             st.write("No business details found for your query.")


# # #########Original Code:
# # import streamlit as st
# # import requests
# # import pandas as pd
# # import os
# # from dotenv import load_dotenv

# # load_dotenv()
# # BASE_URL = os.getenv("BASE_URL")

# # import streamlit as st
# # import requests
# # import pandas as pd
# # import os
# # from dotenv import load_dotenv

# # load_dotenv()
# # BASE_URL = os.getenv("BASE_URL")

# # def show_chatbot(token):
# #     # Initialize session state for conversation history
# #     if 'conversation_history' not in st.session_state:
# #         st.session_state['conversation_history'] = []

# #     st.title("Review Query Chatbot")

# #     # Chat interface
# #     with st.form("chat_form"):
# #         user_input = st.text_input("Ask a question about reviews:")
# #         submit_button = st.form_submit_button("Send")

# #     # Process input when the form is submitted
# #     if submit_button and user_input:
# #         # Add user query to conversation history
# #         st.session_state['conversation_history'].append(f"You: {user_input}")

# #         # Send request to backend
# #         response = requests.post(f"{BASE_URL}/chatbot/", json={"text": user_input})
# #         if response.status_code == 200:
# #             bot_response = response.json()["response"]
# #             # Add bot response to conversation history
# #             st.session_state['conversation_history'].append(f"YelpOBot: {bot_response}")
# #         else:
# #             st.error("Error in processing the query")

# #     # Display conversation history
# #     st.write("Conversation History:")
# #     for message in st.session_state['conversation_history']:
# #         st.text(message)

# import streamlit as st
# import requests
# import os
# from dotenv import load_dotenv

# # Load environment variables
# load_dotenv()
# BASE_URL = os.getenv("BASE_URL")

# def show_chatbot(token):
#     # Initialize session state for conversation history
#     if 'conversation_history' not in st.session_state:
#         st.session_state['conversation_history'] = []

#     st.title("Review Query Chatbot")

#     # Chat interface
#     with st.form("chat_form"):
#         user_input = st.text_input("Ask a question about reviews:")
#         submit_button = st.form_submit_button("Send")

#     # Process input when the form is submitted
#     if submit_button and user_input:
#         # Add user query to conversation history
#         st.session_state['conversation_history'].append(f"You: {user_input}")

#         # Send request to backend
#         response = requests.post(f"{BASE_URL}/chatbot/", json={"text": user_input})
#         if response.status_code == 200:
#             bot_response = response.json()["response"]
#             # Add bot response to conversation history
#             st.session_state['conversation_history'].append(f"YelpOBot: {bot_response}")
#         else:
#             st.error("Error in processing the query")

#     # Display conversation history
#     st.write("Conversation History:")
#     for message in st.session_state['conversation_history']:
#         st.text(message)

    

    # Define the knowledge base text
    knowledge_base = """
    Pennsylvania's culinary landscape features standout eateries that offer memorable dining experiences, such as:
    - Zahav at 237 St James Pl, Philadelphia: Modern Israeli cuisine, known for Lamb Shoulder.
    - Vetri Cucina at 1312 Spruce St, Philadelphia: Exquisite Italian cuisine, famous for Spinach Gnocchi.
    - DiAnoia's Eatery at 2549 Penn Ave, Pittsburgh: Italian fare, popular for Gnocchi Sorrentina.
    - Primanti Bros. at 46 18th St, Pittsburgh: Famous for sandwiches with grilled meat, coleslaw, tomato, and French fries.
    - Talula's Garden at 210 W Washington Square, Philadelphia: Farm-to-table experience, known for Seared Sea Scallops.
    - Morimoto at 723 Chestnut St, Philadelphia: Contemporary Japanese, offering an Omakase menu.
    - Pamela's Diner in Pittsburgh: Known for its fluffy, crepe-like pancakes.
    - Reading Terminal Market at 1136 Arch St, Philadelphia: A variety of local and international foods including the famous Philly Cheesesteak.
    - Le Virtù at 1927 E Passyunk Ave, Philadelphia: Authentic Abruzzese Italian cuisine, known for Maccheroni alla Mugnaia.
    Sushi Restaurants:
    Morimoto - Located at 723 Chestnut St, Philadelphia, Chef Masaharu Morimoto's namesake restaurant offers an innovative sushi experience. The Omakase menu is particularly renowned, taking diners on a chef-curated journey of fresh and inventive sushi.
    Umi - Situated in Pittsburgh at 5849 Ellsworth Ave, Umi is known for its exceptional sushi and sashimi. The restaurant's signature dish, the Toro Tartare, served with caviar and gold leaf, showcases the chef's precision and creativity.
    Zama - At 128 S 19th St, Philadelphia, Zama offers a contemporary sushi experience. Their Philly Special Roll, featuring smoked salmon, cream cheese, and cucumber, is a local twist on the classic sushi roll.
    Fuji Mountain - This sushi destination at 2030 Chestnut St, Philadelphia, is celebrated for its extensive sushi menu and traditional Japanese ambiance. The Dragon Roll, with eel and avocado, is a patron favorite.
    Sakura Japanese Restaurant - Located at 641 Monroe St, Stroudsburg, Sakura is a hidden gem offering authentic Japanese sushi. The Spicy Tuna Roll here is a must-try for those who love a blend of spice and fresh flavors.
    Pizza Places:
    Pizzeria Vetri - Found at 1939 Callowhill St, Philadelphia, this pizzeria is famous for its authentic Neapolitan pizzas. The Margherita Pizza, with its perfectly charred crust and fresh ingredients, stands out for its simplicity and flavor.
    Angelo's Pizzeria - Located in South Philly at 736 S 9th St, Angelo's is known for its hearty and flavorful pizzas. The Upside Down Jawn, with its unique crust and sauce on top, is a local favorite.
    Mineo's Pizza House - A staple in Pittsburgh at 2128 Murray Ave, Mineo's offers a classic, no-frills pizza experience. Their pepperoni pizza, known for its crispy crust and generous toppings, is a must-try.
    Lorenzo and Sons Pizza - Situated on 305 South St, Philadelphia, Lorenzo’s is famous for its massive, cheesy slices. The plain cheese pizza here is a true testament to the adage 'less is more.'
    Tacconelli's Pizzeria - At 2604 E Somerset St, Philadelphia, Tacconelli's is known for its thin-crust pizzas. The White Pizza, a blend of mozzarella, garlic, and spinach, is a unique and flavorful option.
    """

    # Function to interact with OpenAI's text completion model
    def get_openai_response(prompt):
        
        openai.api_key = os.environ.get('API_KEY') 
        response = openai.Completion.create(
            engine="davinci",
            prompt=prompt,
            max_tokens=150
        )
        return response.choices[0].text.strip()

    # Streamlit UI
    st.title("YelpoBot Explorer Chatbot")

    user_input = st.text_input("Ask me about restaurants in Pennsylvania:", "")

    if user_input:
        # Append the user input to the knowledge base text
        full_prompt = knowledge_base + "\nUser asks: " + user_input
        # Get the response from OpenAI
        response = get_openai_response(full_prompt)
        # Display the response
        st.text("YelpoBot Explorer says:")
        st.write(response)