from tasks.create_ai_agent import create_conversation_ai_agent

def test_create_agent():
    """Test creating a new AI agent from JSON file"""
    print("🚀 Testing AI Agent Creation from JSON...")
    
    # Call the function with default agent.json file
    result = create_conversation_ai_agent()
    
    if result and result.get("status") == "success":
        print("✅ Agent created successfully!")
        print(f"🆔 Agent ID: {result.get('agent_id')}")
        print(f"📛 Agent Name: {result.get('name')}")
        print(f"� Source File: {result.get('source_file')}")
        print(f"⏰ Created At: {result.get('created_at')}")
        return result.get('agent_id')
    else:
        print("❌ Failed to create agent:")
        print(f"Error: {result.get('message', 'Unknown error')}")
        if result.get('details'):
            print(f"Details: {result.get('details')}")
        return None

if __name__ == "__main__":
    print("🤖 Testing AI Agent Creation")
    print("=" * 30)
    
    # Test creating agent from JSON file
    agent_id = test_create_agent()
    
    print("\n" + "=" * 30)
    if agent_id:
        print(f"✨ Created agent ID: {agent_id}")
        print("🏁 Test completed successfully!")
    else:
        print("❌ Test failed!")
    print("=" * 30)
