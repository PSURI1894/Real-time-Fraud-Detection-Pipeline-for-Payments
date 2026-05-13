import torch
import torch.nn as nn
import mlflow

class TabNetEncoder(nn.Module):
    def __init__(self, input_dim):
        super().__init__()
        self.fc = nn.Sequential(
            nn.Linear(input_dim, 64),
            nn.ReLU(),
            nn.Linear(64, 32),
            nn.ReLU(),
            nn.Linear(32, 2)
        )
        
    def forward(self, x):
        return self.fc(x)

def train_neural():
    mlflow.set_experiment("fraud_detection_tabnet")
    with mlflow.start_run():
        model = TabNetEncoder(input_dim=3)
        optimizer = torch.optim.Adam(model.parameters(), lr=0.001)
        
        mlflow.log_param("optimizer", "Adam")
        mlflow.log_param("neural_layers", "3")
        
        torch.save(model.state_dict(), "tabnet.pt")
        mlflow.log_artifact("tabnet.pt")
        print("TabNet model compiled.")

if __name__ == "__main__":
    train_neural()
