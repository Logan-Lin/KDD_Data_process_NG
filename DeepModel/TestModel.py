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
