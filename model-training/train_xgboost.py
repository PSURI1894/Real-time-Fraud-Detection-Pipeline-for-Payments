import xgboost as xgb
import mlflow
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, average_precision_score

def train():
    mlflow.set_experiment("fraud_detection_xgboost")
    with mlflow.start_run():
        df = pd.DataFrame({
            'recent_card_velocity_1m': [1, 5, 2, 45, 1, 80],
            'card_amount_sum_1h': [10.5, 4500.0, 30.0, 12000.0, 5.0, 25000.0],
            'merchant_chargeback_rate_24h': [0.01, 0.22, 0.01, 0.35, 0.0, 0.44],
            'is_fraud': [0, 1, 0, 1, 0, 1]
        })
        
        X = df.drop('is_fraud', axis=1)
        y = df['is_fraud']
        
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)
        
        model = xgb.XGBClassifier(
            max_depth=6,
            learning_rate=0.1,
            n_estimators=100,
            scale_pos_weight=15.0
        )
        
        model.fit(X_train, y_train)
        preds = model.predict(X_test)
        
        ap = average_precision_score(y_test, preds)
        mlflow.log_metric("pr_auc", ap)
        mlflow.xgboost.log_model(model, "model")
        print(f"XGBoost Model trained with PR-AUC score: {ap:.4f}")

if __name__ == "__main__":
    train()
