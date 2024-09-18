import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, classification_report
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.preprocessing import StandardScaler
import joblib


file_path = '/Users/lucianoleroi/Desktop/DataVisionAnalytics/joined_data.xlsx'
df = pd.read_excel(file_path)

# Nettoyer les données
df = df.dropna()  
df = pd.get_dummies(df, columns=['driver', 'constructor', 'season', 'name', 'status', 'status_sprint', 'wind_direction'])
df['is_winner'] = (df['finishing_position'] == 1).astype(int)


features = ['grid_position', 'points', 'laps', 'fastestLap', 'fastestLapTime', 'fastestLapSpeed', 
             'grid_position_sprint', 'points_sprint', 'laps_sprint', 'fastestLap_sprint', 
             'fastestLapTime_sprint', 'finishing_position_qualifying', 'q1_best_time', 
             'q2_best_time', 'q3_best_time', 'air_temperature', 'humidity', 'pressure', 
             'rainfall', 'track_temperature', 'wind_speed']

X = df[features]
y = df['is_winner']
X = X.apply(pd.to_numeric, errors='coerce')
X = X.fillna(0) 
X.replace([float('inf'), -float('inf')], 0, inplace=True)
scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)
X_scaled = np.nan_to_num(X_scaled, nan=0, posinf=0, neginf=0)

# Entraîner le modèle avec validation croisée
model = RandomForestClassifier(n_estimators=100, random_state=42)
cross_val_scores = cross_val_score(model, X_scaled, y, cv=5)  # Validation croisée à 5 plis
print(f'Cross-validation scores: {cross_val_scores}')
print(f'Mean cross-validation score: {cross_val_scores.mean()}')

# Diviser les données en ensembles d'entraînement et de test
X_train, X_test, y_train, y_test = train_test_split(X_scaled, y, test_size=0.2, random_state=42)
model.fit(X_train, y_train)
predictions = model.predict(X_test)
accuracy = accuracy_score(y_test, predictions)
print(f'Accuracy: {accuracy:.2f}')
print(classification_report(y_test, predictions))
pd.DataFrame(X_train, columns=features).to_csv('X_train.csv', index=False)
pd.DataFrame(y_train).to_csv('y_train.csv', index=False)
pd.DataFrame(X_test, columns=features).to_csv('X_test.csv', index=False)
pd.DataFrame(y_test).to_csv('y_test.csv', index=False)

# Sauvegarder le scaler et le modèle
joblib.dump(scaler, 'scaler.pkl')
joblib.dump(model, 'model.pkl')

