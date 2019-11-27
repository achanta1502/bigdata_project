from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.stem.porter import PorterStemmer
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, recall_score, f1_score
from sklearn.model_selection import train_test_split
from sklearn.svm import SVC
from sklearn.neural_network import MLPClassifier
from sklearn.naive_bayes import MultinomialNB
from os import path
import pickle
import pandas as pd

modelpath = 'model.sav'
vectorpath = 'vector.sav'

# model = LogisticRegression(solver='newton-cg', multi_class='multinomial')
# model = MultinomialNB()
# model = SVC(kernel='poly')
model = MLPClassifier(activation='logistic', learning_rate='adaptive')


def tokenize(text):
    tokens = set(word_tokenize(text))
    stop_words = stopwords_removal()
    tokens = [w.lower() for w in tokens if not w in stop_words]
    stems = [porter(item) for item in tokens]
    return stems


vect = TfidfVectorizer(tokenizer=tokenize, use_idf=True)
port = PorterStemmer()


def data_from_text():
    df = pd.read_csv("Data/2CVTweets/Seattle.csv", delimiter=';', header=None)
    df.columns = ['number', 'text', 'label']
    return df


def text_processing():
    df = data_from_text()
    return df


def model_building():
    df = text_processing()
    x_train, y_train, x_test, y_test = train_test(df, 0.1)
    train_vectors, test_vectors = vector_fit_transform(x_train, x_test)
    train_model(train_vectors, y_train)
    predicted = predict(test_vectors)
    print(accuracy(predicted, y_test))


def save_vector():
    pickle.dump(vect, open(vectorpath, "wb"))


def load_vector():
    global vect
    vect = pickle.load(open(vectorpath, "rb"))


def save_model():
    pickle.dump(model, open(modelpath, "wb"))


def load_model():
    print("loading module")
    global model
    model = pickle.load(open(modelpath, 'rb'))


def get_model():
    return model


def train_model(x_train, y_train):
    model.fit(x_train, y_train)
    save_model()


def predict(test):
    return model.predict(test)


def accuracy(predicted, original):
    return accuracy_score(predicted, original)


def stopwords_removal():
    return set(stopwords.words('english'))


def porter(word):
    return port.stem(word)


def fit_transform(train):
    fit = vect.fit_transform(train)
    save_vector()
    return fit


def transform(test):
    return vect.transform(test)


def vector_fit_transform(train, test):
    train_vectors = fit_transform(train)
    test_vectors = transform(test)
    return train_vectors, test_vectors


def train_test(df, size):
    train, test = train_test_split(df, test_size=size)
    X_train = train.loc[:, 'text'].values
    Y_train = train.loc[:, 'label'].values
    X_test = test.loc[:, 'text'].values
    Y_test = test.loc[:, 'label'].values
    return X_train, Y_train, X_test, Y_test


def test_data(df):
    X_test = df.loc[:, 'text'].values
    x_test = [str(x) for x in X_test]
    return x_test


def pipeline(df):
    features = test_data(df)
    test_vectors = transform(features)
    predicted = predict(test_vectors)
    res = {
        "predicted": predicted.flatten().tolist()
    }
    return res


def model_start():
    if path.exists(modelpath) and path.exists(vectorpath):
        load_vector()
        load_model()
    else:
        model_building()
