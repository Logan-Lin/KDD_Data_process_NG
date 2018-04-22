from keras import backend as k
from keras.callbacks import Callback
import numpy as np


def mean_squared_error(y_true, y_pred):
    return k.mean(k.square(y_pred - y_true))


def rmse(y_true, y_pred):
    return mean_squared_error(y_true, y_pred) ** 0.5


class LossHistory(Callback):
    def on_train_begin(self, logs={}):
        self.losses = []

    def on_epoch_end(self, batch, logs={}):
        self.losses.append(logs.get('loss'))


# X_meo = (#,h,5,n,n): max-min normalization:[-1,1]
# X_aq = (#,h,6): log_e, max-min normalization:[-1,1]
def transform_X(X_meo, X_aq):
    X_meo_new = np.zeros(X_meo.shape)
    X_aq_new = np.zeros(X_aq.shape)
    print('>> normalize data')
    print('>> ... meo X')
    meo_max = []
    meo_min = []
    for i in range(5):
        _min = X_meo[:, :, i, :, :].min()
        _max = X_meo[:, :, i, :, :].max()
        X_meo_new[:, :, i, :, :] = 1. * (X_meo[:, :, i, :, :] - _min) / (_max - _min)
        X_meo_new[:, :, i, :, :] = X_meo_new[:, :, i, :, :] * 2. - 1.
        meo_max.append(_max)
        meo_min.append(_min)
    print('>> ... aq X')
    aq_min = []
    aq_max = []
    X_aq = np.log(X_aq)
    for i in range(6):
        _min = X_aq[:, :, i].min()
        _max = X_aq[:, :, i].max()
        aq_min.append(_min)
        aq_max.append(_max)
        X_aq_new[:, :, i] = 1. * (X_aq[:, :, i] - _min) / (_max - _min)
        X_aq_new[:, :, i] = X_aq_new[:, :, i] * 2. - 1.
    return X_meo_new, X_aq_new, meo_max, meo_min, aq_max, aq_min


def invert_transform_X(X_meo, X_aq, meo_max, meo_min, aq_max, aq_min):
    X_meo_new = np.zeros(X_meo.shape)
    X_aq_new = np.zeros(X_aq.shape)
    print('>> invert_transform_X data')
    print('>> ... meo X')
    for i in range(5):
        _min = meo_min[i]
        _max = meo_max[i]
        X_meo_new[:, :, i, :, :] = (X_meo[:, :, i, :, :] + 1.) / 2.
        X_meo_new[:, :, i, :, :] = 1. * X_meo_new[:, :, i, :, :] * (_max - _min) + _min
    print('>> ... aq X')
    for i in range(6):
        _min = aq_min[i]
        _max = aq_max[i]
        X_aq_new[:, :, i] = (X_aq[:, :, i] + 1.) / 2.
        X_aq_new[:, :, i] = 1. * X_aq_new[:, :, i] * (_max - _min) + _min
    X_aq_new = np.exp(X_aq_new)
    return X_meo_new, X_aq_new


# all_Y = (#,48,3)
def transform_Y(Y, aq_max, aq_min):
    Y_new = np.zeros(Y.shape)
    Y_new = np.log(Y)
    for i in range(3):
        Y_new[:, :, i] = 1. * (Y_new[:, :, i] - aq_min[i]) / (aq_max[i] - aq_min[i])
        Y_new[:, :, i] = Y_new[:, :, i] * 2. - 1.
    return Y_new


def inverse_transform_Y(Y, aq_max, aq_min):
    Y_new = np.zeros(Y.shape)
    for i in range(3):
        Y_new[:, :, i] = (Y[:, :, i] + 1.) / 2.
        Y_new[:, :, i] = 1. * Y_new[:, :, i] * (aq_max[i] - aq_min[i]) + aq_min[i]
    Y_new = np.exp(Y_new)
    return Y_new


def splitTrainandTest(all_timestampes_Y, test_strat_date):
    for index in range(all_timestampes_Y.shape[0]):
        if all_timestampes_Y[index] == test_strat_date:
            return index
