def prompt(question):
    """
    Helper function to get user confirmation
    """
    while True:
        response = input(f"{question} [y/n]: ").strip().lower()
        if response in ['y', 'Y', 'yes']:
            return True
        elif response in ['n', 'N', 'no']:
            return False
        print("Please respond with y/yes or n/no.")

__all__ = ["prompt"]
