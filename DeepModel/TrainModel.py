import os
import time
from keras.callbacks import ModelCheckpoint
from DeepModel.AQNet_many2many import *
from DeepModel.utils import *


class TrainModel(object):

    def __init__(self, data_path, lr, batch_size, nb_end_epoch, nb_start_epoch=0):
        self.data_path = data_path
        self.batch_size = batch_size
        self.lr = lr
        self.nb_start_epoch = nb_start_epoch  # 用validation找到早停电之前，预训练的轮次；
        self.nb_end_epoch = nb_end_epoch

    def train_AQNet_cnnlstm_lstm_embedding_normalvalue(self, X, Y, len_history, meo_size, nb_meo_lstm_encode,
                                                       nb_meo_lstm_decode, nb_aq_lstm_encode, nb_aq_lstm_decode,
                                                       external_dim, nb_meo_conv=2):
        model = AQNet_cnnlstm_lstm_embedding_normalvalue(meo_conf=(len_history, 5, meo_size, meo_size),
                                                         hisAQ_conf=(len_history, 6), external_dim=external_dim,
                                                         nb_meo_conv=2, nb_meo_lstm_encode=nb_meo_lstm_encode,
                                                         nb_meo_lstm_decode=nb_meo_lstm_decode,
                                                         nb_aq_lstm_encode=nb_aq_lstm_encode,
                                                         nb_aq_lstm_decode=nb_aq_lstm_decode)
        adam = Adam(lr=0.00001)
        model.compile(loss='mse', optimizer=adam, metrics=[rmse])
        hyperparams_name = 'cnnlstm_lstm_embedding_normalvalue.h{}.meo_size{}.' \
                           'meo_en{}.meo_de{}.aq_en{}.aq_de{}.ex_dim{}'.format(
            len_history, meo_size, nb_meo_lstm_encode, nb_meo_lstm_decode, nb_aq_lstm_encode, nb_aq_lstm_decode,
            external_dim)

        model.summary()

        path = os.path.join(self.data_path, 'MODEL', hyperparams_name)

        if not os.path.exists(path):
            os.makedirs(path)
            print("mkdir path:", path)

        # load nb_start_epoch model
        if self.nb_start_epoch > 0:
            fname_param = os.path.join(path, 'weights.{}.hdf5'.format(self.nb_start_epoch))
            print("load nb_start_epoch model: ", fname_param)
            model.load_weights(fname_param)

        png_fname = os.path.join('{}.png'.format(hyperparams_name))
        plot_model(model, png_fname, show_shapes=True)
        # begin train model
        print('begin fit the model')
        start_fit_time = time.time()

        fname_param = os.path.join(path, 'weights.{epoch:d}.hdf5')
        model_checkpoint = ModelCheckpoint(fname_param, monitor='loss', verbose=1, save_best_only=True, mode='min',
                                           period=1)
        history = LossHistory()
        model.fit(X, Y, nb_epoch=self.nb_end_epoch, initial_epoch=self.nb_start_epoch, verbose=1,
                  batch_size=self.batch_size, callbacks=[model_checkpoint, history])
        elapsed = time.time() - start_fit_time
        print('train from {} epoch to {} epoch cost {}s'.format(self.nb_start_epoch, self.nb_end_epoch, elapsed))
        print("----------- print AQNet_cnnlstm_lstm_embedding_realvalue train loss ----------")
        for i, iloss in enumerate(history.losses):
            print("epoch %d: %f" % ((i + 1), iloss))
