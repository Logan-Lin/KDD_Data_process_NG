from keras import backend as k
from keras.callbacks import Callback


def mean_squared_error(y_true, y_pred):
    return k.mean(k.square(y_pred - y_true))


def rmse(y_true, y_pred):
    return mean_squared_error(y_true, y_pred) ** 0.5


class LossHistory(Callback):
    def on_train_begin(self, logs={}):
        self.losses = []

    def on_epoch_end(self, batch, logs={}):
        self.losses.append(logs.get('loss'))


# X_meo = (#,h,5,n,n)
# X_aq = (#,h,6)
def transform_X(X_meo, X_aq):
    print('>> normalize data')

    print('>> ... meo X')

    meo_max = []
    meo_min = []

    for i in range(5):
        _min = X_meo[:, :, i, :, :].min()
        _max = X_meo[:, :, i, :, :].max()
        X_meo[:, :, i, :, :] = 1. * (X_meo[:, :, i, :, :] - _min) / (_max - _min)
        X_meo[:, :, i, :, :] = X_meo[:, :, i, :, :] * 2. - 1.
        meo_max.append(_max)
        meo_min.append(_min)

    print('>> ... aq X')

    aq_min = []
    aq_max = []

    for i in range(6):
        _min = X_aq[:, :, i].min()
        _max = X_aq[:, :, i].max()
        aq_min.append(_min)
        aq_max.append(_max)
        X_aq[:, :, i] = 1. * (X_aq[:, :, i] - _min) / (_max - _min)
        X_aq[:, :, i] = X_aq[:, :, i] * 2. - 1.

    return X_meo, X_aq, meo_max, meo_min, aq_max, aq_min


# all_Y = (#,48,3)
def transform_Y(Y, aq_max, aq_min):
    for i in range(3):
        Y[:, :, i] = 1. * (Y[:, :, i] - aq_max[i]) / (aq_max[i] - aq_min[i])
        Y[:, :, i] = Y[:, :, i] * 2. - 1.

    return Y


def inverse_transform_Y(Y, aq_max, aq_min):
    for i in range(3):
        Y[:, :, i] = (Y[:, :, i] + 1.) / 2.
        Y[:, :, i] = 1. * Y[:, :, i] * (aq_max[i] - aq_min[i]) + aq_min[i]
    return Y
