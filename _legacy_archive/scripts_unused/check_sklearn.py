print("Start sklearn check...")
try:
    from sklearn.tree import DecisionTreeClassifier
    print("Sklearn ok")
except ImportError:
    print("Sklearn missing")
except Exception as e:
    print(f"Sklearn error: {e}")
print("End check")
