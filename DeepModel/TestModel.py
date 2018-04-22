import sys

sys.path.append('../')
from DeepModel.AQNet_many2many import *
from DeepModel.evaluate import *
import os
from DeepModel.utils import *


class TestModel(object):

    def __init__(self, model_path, batch_size, nb_start_epoch=0, Y_aq_max=0, Y_aq_min=0):
        self.model_path = model_path
        self.batch_size = batch_size
        self.nb_start_epoch = nb_start_epoch
        self.Y_aq_max = Y_aq_max
        self.Y_aq_min = Y_aq_min

    def test_AQNet_cnnlstm_lstm_embedding_normalvalue(self, X, Y, timestamps, len_history, meo_size, nb_meo_lstm_encode,
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

        hyperparams_name = 'cnnlstm_lstm_embedding_normalvalue.h{}.meo_size{}.meo_en{}.meo_de{}.aq_en{}.aq_de{}.ex_dim{}'.format(
            len_history, meo_size, nb_meo_lstm_encode, nb_meo_lstm_decode, nb_aq_lstm_encode, nb_aq_lstm_decode,
            external_dim)

        path = os.path.join(self.model_path, 'MODEL', hyperparams_name)
        result_path = os.path.join(self.model_path, 'RESULTS', hyperparams_name)

        if os.path.exists(result_path) == False:
            os.makedirs(result_path)
            print("mkdir result_path:", result_path)

        all_epoch = []
        all_normal_mse = []
        all_real_rmse = []
        all_real_smape = []
        # test all cont model
        for parent, dirnames, filenames in os.walk(path):
            for filename in filenames:
                # print('filename:',filename)
                epoch_index = int(filename.split(".")[1])
                # if (epoch_index > self.nb_start_epoch) and ((epoch_index%10)==0):
                if epoch_index > self.nb_start_epoch:
                    print("epoch_index:", epoch_index)
                    all_epoch.append(epoch_index)
                    read_filename = os.path.join(parent, filename)
                    model.load_weights(read_filename)
                    Y_predic = model.predict(X, batch_size=self.batch_size, verbose=1)
                    print('Y_predic shape:', Y_predic.shape)
                    normal_mse, normal_rmse = computeRMSE(Y_predic, Y)
                    Y_real = inverse_transform_Y(Y, self.Y_aq_max, self.Y_aq_min)
                    Y_predict_real = inverse_transform_Y(Y_predic, self.Y_aq_max, self.Y_aq_min)
                    real_mse, real_rmse = computeRMSE(Y_predict_real, Y_real)
                    real_smape = computeSMAPE(Y_predict_real, Y_real)
                    all_normal_mse.append(normal_mse)
                    all_real_rmse.append(real_rmse)
                    all_real_smape.append(real_smape)
        info = {}
        for i in range(len(all_epoch)):
            info[all_epoch[i]] = str(all_normal_mse[i]) + "," + str(all_real_rmse[i]) + "," + str(all_real_smape[i])
        info_list = sorted(info.items(), key=lambda x: x[0], reverse=False)
        print("epoch, normal_mse, real_rmse, real_smape")
        for info_item in info_list:
            print(info_item)

    def test_AQNet_cnnlstm_lstm_embedding_normalvalue_saveResults(self, meo_max, meo_min, aq_max, aq_min, X, Y,
                                                                  timestamps, len_history, meo_size, nb_meo_lstm_encode,
                                                                  nb_meo_lstm_decode, nb_aq_lstm_encode,
                                                                  nb_aq_lstm_decode,
                                                                  external_dim, nb_meo_conv=2, fixed_epoch=1):
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
        path = os.path.join(self.model_path, 'MODEL', hyperparams_name)
        result_path = os.path.join(self.model_path, 'RESULTS', hyperparams_name)
        if os.path.exists(result_path) == False:
            os.makedirs(result_path)
            print("mkdir result_path:", result_path)
        all_epoch = []
        all_normal_mse = []
        all_real_rmse = []
        all_real_smape = []
        # test all cont model
        for parent, dirnames, filenames in os.walk(path):
            for filename in filenames:
                # print('filename:',filename)
                epoch_index = int(filename.split(".")[1])
                if epoch_index == fixed_epoch:
                    print("epoch_index:", epoch_index)
                    all_epoch.append(epoch_index)
                    read_filename = os.path.join(parent, filename)
                    model.load_weights(read_filename)
                    Y_predic = model.predict(X, batch_size=self.batch_size, verbose=1)
                    print('Y_predic shape:', Y_predic.shape)  # (#, predict_interval, 3)
                    normal_mse, normal_rmse = computeRMSE(Y_predic, Y)
                    Y_real = inverse_transform_Y(Y, self.Y_aq_max, self.Y_aq_min)
                    Y_predict_real = inverse_transform_Y(Y_predic, self.Y_aq_max, self.Y_aq_min)
                    real_mse, real_rmse = computeRMSE(Y_predict_real, Y_real)
                    real_smape = computeSMAPE(Y_predict_real, Y_real)
                    all_normal_mse.append(normal_mse)
                    all_real_rmse.append(real_rmse)
                    all_real_smape.append(real_smape)

                    # save predicted results

                    # Y_real:(#, predict_interval, 3)
                    # Y_predict_real:(#, predict_interval, 3)
                    # X[1]:(#,len_history,6)
                    X[0], X[1] = invert_transform_X(X[0], X[1], meo_max, meo_min, aq_max, aq_min)
                    print("X[1].shape:", X[1].shape)
                    # PM2.5
                    saveResults(os.path.join(result_path, 'PM2.5_{}.txt'.format(fixed_epoch)), Y_predict_real[:, :, 0],
                                Y_real[:, :, 0], X[1][:, :, 0])
                    # PM10
                    saveResults(os.path.join(result_path, 'PM10_{}.txt'.format(fixed_epoch)), Y_predict_real[:, :, 1],
                                Y_real[:, :, 1], X[1][:, :, 1])
                    # O3
                    saveResults(os.path.join(result_path, 'O3{}.txt'.format(fixed_epoch)), Y_predict_real[:, :, 2],
                                Y_real[:, :, 2], X[1][:, :, 4])
        info = {}
        for i in range(len(all_epoch)):
            info[all_epoch[i]] = str(all_normal_mse[i]) + "," + str(all_real_rmse[i]) + "," + str(all_real_smape[i])

        info_list = sorted(info.items(), key=lambda x: x[0], reverse=False)

        print("epoch, normal_mse, real_rmse, real_smape")
        for info_item in info_list:
            print(info_item)
