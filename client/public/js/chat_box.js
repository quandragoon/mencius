function ChatBox() {
	this.messages = []	// List of all messages displayed in chat box

	// Adds message to message list
	this.addMessage = function(message) {
		this.messages.push(message);
	};
}
