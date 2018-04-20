import keras
from keras.layers import (
    Input,
    Dense,
    Flatten,
    TimeDistributed,
    RepeatVector
)
from keras.layers import LSTM
from keras.layers.convolutional import Conv2D
from keras.models import Model
from keras.optimizers import Adam
from keras.utils import plot_model

predict_interval = 50

'''
	meo_conf: (history_length,channel,width,height)  
	hisAQ_conf: (history_length(timesteps),dim)
	nb_meo_conv : number of CNN in meo_part
	nb_meo_lstm : number of LSTM in meo_part
	nb_aq_lstm : number of LSTM in aq_part
'''


def AQNet_cnnlstm_lstm_embedding_realvalue(meo_conf=(3, 5, 3, 3), hisAQ_conf=(3, 6), external_dim=8, nb_meo_conv=2,
                                           nb_meo_lstm_encode=1, nb_meo_lstm_decode=1, nb_aq_lstm_encode=1,
                                           nb_aq_lstm_decode=1):
    main_inputs = []
    main_outputs = []
    # the first meo_part
    input = Input(shape=meo_conf)
    main_inputs.append(input)
    conv = input
    for nb in range(nb_meo_conv):
        conv = TimeDistributed(Conv2D(filters=32, kernel_size=(3, 3), activation="relu", padding='same'))(conv)
    flatten = TimeDistributed(Flatten())(conv)
    dense1 = TimeDistributed(Dense(128, activation='relu'))(flatten)
    lstm_1 = dense1
    for nb in range(nb_meo_lstm_encode - 1):
        lstm_1 = LSTM(units=64, return_sequences=True, activation="relu")(lstm_1)
    lstm_1 = LSTM(units=64, return_sequences=False, activation="relu")(lstm_1)
    repeat_lstm_1 = RepeatVector(predict_interval)(lstm_1)
    for nb in range(nb_meo_lstm_decode):
        repeat_lstm_1 = LSTM(units=64, return_sequences=True, activation="relu")(repeat_lstm_1)
    main_outputs.append(repeat_lstm_1)

    # the second aq_part
    input = Input(shape=hisAQ_conf)
    main_inputs.append(input)
    lstm_2 = input
    for nb in range(nb_aq_lstm_encode - 1):
        lstm_2 = LSTM(units=64, return_sequences=True, activation="relu")(lstm_2)
    lstm_2 = LSTM(units=64, return_sequences=False, activation="relu")(lstm_2)
    repeat_lstm_2 = RepeatVector(predict_interval)(lstm_2)
    for nb in range(nb_aq_lstm_decode):
        repeat_lstm_2 = LSTM(units=64, return_sequences=True, activation="relu")(repeat_lstm_2)
    main_outputs.append(repeat_lstm_2)

    # the third external_part
    if external_dim > 0:
        input = Input(shape=(external_dim,))
        main_inputs.append(input)
        dense = Dense(32, activation='relu')(input)
        dense = Dense(32, activation='relu')(dense)
        repeat_dense = RepeatVector(predict_interval)(dense)
        main_outputs.append(repeat_dense)

    # concatenate three features
    all_features = keras.layers.concatenate(main_outputs, axis=-1)
    all_dense = TimeDistributed(Dense(32, activation='relu'))(all_features)
    output = TimeDistributed(Dense(3, activation='relu'))(all_dense)
    model = Model(input=main_inputs, output=output)
    return model


def AQNet_cnnlstm_lstm_embedding_normalvalue(meo_conf=(3, 5, 3, 3), hisAQ_conf=(3, 6), external_dim=8, nb_meo_conv=2,
                                             nb_meo_lstm_encode=5, nb_meo_lstm_decode=1, nb_aq_lstm_encode=1,
                                             nb_aq_lstm_decode=1):
    main_inputs = []
    main_outputs = []

    # the first meo_part
    input = Input(shape=meo_conf)
    main_inputs.append(input)
    conv = input
    for nb in range(nb_meo_conv):
        conv = TimeDistributed(Conv2D(filters=32, kernel_size=(3, 3), activation="relu", padding='same'))(conv)
    flatten = TimeDistributed(Flatten())(conv)
    dense1 = TimeDistributed(Dense(128, activation='relu'))(flatten)
    lstm_1 = dense1
    for nb in range(nb_meo_lstm_encode - 1):
        lstm_1 = LSTM(units=64, return_sequences=True, activation="relu")(lstm_1)
    lstm_1 = LSTM(units=64, return_sequences=False, activation="relu")(lstm_1)
    repeat_lstm_1 = RepeatVector(predict_interval)(lstm_1)
    for nb in range(nb_meo_lstm_decode):
        repeat_lstm_1 = LSTM(units=64, return_sequences=True, activation="relu")(repeat_lstm_1)
    main_outputs.append(repeat_lstm_1)

    # the second aq_part
    input = Input(shape=hisAQ_conf)
    main_inputs.append(input)
    lstm_2 = input
    for nb in range(nb_aq_lstm_encode - 1):
        lstm_2 = LSTM(units=64, return_sequences=True, activation="relu")(lstm_2)
    lstm_2 = LSTM(units=64, return_sequences=False, activation="relu")(lstm_2)
    repeat_lstm_2 = RepeatVector(predict_interval)(lstm_2)
    for nb in range(nb_aq_lstm_decode):
        repeat_lstm_2 = LSTM(units=64, return_sequences=True, activation="relu")(repeat_lstm_2)
    main_outputs.append(repeat_lstm_2)

    # the third external_part
    if external_dim > 0:
        input = Input(shape=(external_dim,))
        main_inputs.append(input)
        dense = Dense(32, activation='relu')(input)
        dense = Dense(32, activation='relu')(dense)
        repeat_dense = RepeatVector(predict_interval)(dense)
        main_outputs.append(repeat_dense)

    # concatenate three features
    all_features = keras.layers.concatenate(main_outputs, axis=-1)
    all_dense = TimeDistributed(Dense(32, activation='relu'))(all_features)
    output = TimeDistributed(Dense(3, activation='tanh'))(all_dense)
    model = Model(input=main_inputs, output=output)
    return model


'''
  meo_part and hisAQ_part share the LSTM
'''


def AQNet_cnn_sharelstm_embedding_realvalue(meo_conf=(3, 5, 3, 3), hisAQ_conf=(3, 6), external_dim=8, nb_meo_conv=2,
                                            nb_meo_lstm_encode=1, nb_aq_lstm_encode=1, nb_lstm_decode=1):
    assert (meo_conf[0] == hisAQ_conf[0])
    main_inputs = []
    two_repeat_outputs = []
    main_outputs = []

    # the first meo_part
    input = Input(shape=meo_conf)
    main_inputs.append(input)
    conv = input
    for nb in range(nb_meo_conv):
        conv = TimeDistributed(Conv2D(filters=32, kernel_size=(3, 3), activation="relu", padding='same'))(conv)
    flatten = TimeDistributed(Flatten())(conv)
    dense1 = TimeDistributed(Dense(128, activation='relu'))(flatten)
    lstm_1 = dense1
    for nb in range(nb_meo_lstm_encode - 1):
        lstm_1 = LSTM(units=64, return_sequences=True, activation="relu")(lstm_1)
    lstm_1 = LSTM(units=64, return_sequences=False, activation="relu")(lstm_1)
    repeat_lstm_1 = RepeatVector(predict_interval)(lstm_1)
    two_repeat_outputs.append(repeat_lstm_1)

    # the second aq_part
    input = Input(shape=hisAQ_conf)
    main_inputs.append(input)
    lstm_2 = input
    for nb in range(nb_aq_lstm_encode - 1):
        lstm_2 = LSTM(units=64, return_sequences=True, activation="relu")(lstm_2)
    lstm_2 = LSTM(units=64, return_sequences=False, activation="relu")(lstm_2)
    repeat_lstm_2 = RepeatVector(predict_interval)(lstm_2)
    two_repeat_outputs.append(repeat_lstm_2)

    # concatenate the first two parts and share the LSTM
    two_features = keras.layers.concatenate(two_repeat_outputs, axis=-1)
    for nb in range(nb_lstm_decode):
        two_features = LSTM(units=64, return_sequences=True, activation="relu")(two_features)
    main_outputs.append(two_features)

    # the third external_part
    if external_dim > 0:
        input = Input(shape=(external_dim,))
        main_inputs.append(input)
        dense = Dense(32, activation='relu')(input)
        dense = Dense(32, activation='relu')(dense)
        repeat_dense = RepeatVector(predict_interval)(dense)
        main_outputs.append(repeat_dense)

    # concatenate three features
    all_features = keras.layers.concatenate(main_outputs, axis=-1)
    all_dense = TimeDistributed(Dense(32, activation='relu'))(all_features)
    output = TimeDistributed(Dense(3, activation='relu'))(all_dense)
    model = Model(input=main_inputs, output=output)
    return model


def AQNet_cnn_sharelstm_embedding_normalvalue(meo_conf=(3, 5, 3, 3), hisAQ_conf=(3, 6), external_dim=8, nb_meo_conv=2,
                                              nb_meo_lstm_encode=1, nb_aq_lstm_encode=1, nb_lstm_decode=1):
    assert (meo_conf[0] == hisAQ_conf[0])
    main_inputs = []
    two_repeat_outputs = []
    main_outputs = []

    # the first meo_part
    input = Input(shape=meo_conf)
    main_inputs.append(input)
    conv = input
    for nb in range(nb_meo_conv):
        conv = TimeDistributed(Conv2D(filters=32, kernel_size=(3, 3), activation="relu", padding='same'))(conv)
    flatten = TimeDistributed(Flatten())(conv)
    dense1 = TimeDistributed(Dense(128, activation='relu'))(flatten)
    lstm_1 = dense1
    for nb in range(nb_meo_lstm_encode - 1):
        lstm_1 = LSTM(units=64, return_sequences=True, activation="relu")(lstm_1)
    lstm_1 = LSTM(units=64, return_sequences=False, activation="relu")(lstm_1)
    repeat_lstm_1 = RepeatVector(predict_interval)(lstm_1)
    two_repeat_outputs.append(repeat_lstm_1)

    # the second aq_part
    input = Input(shape=hisAQ_conf)
    main_inputs.append(input)
    lstm_2 = input
    for nb in range(nb_aq_lstm_encode - 1):
        lstm_2 = LSTM(units=64, return_sequences=True, activation="relu")(lstm_2)
    lstm_2 = LSTM(units=64, return_sequences=False, activation="relu")(lstm_2)
    repeat_lstm_2 = RepeatVector(predict_interval)(lstm_2)
    two_repeat_outputs.append(repeat_lstm_2)
    # concatenate the first two parts and share the LSTM
    two_features = keras.layers.concatenate(two_repeat_outputs, axis=-1)
    for nb in range(nb_lstm_decode):
        two_features = LSTM(units=64, return_sequences=True, activation="relu")(two_features)
    main_outputs.append(two_features)

    # the third external_part
    if external_dim > 0:
        input = Input(shape=(external_dim,))
        main_inputs.append(input)
        dense = Dense(32, activation='relu')(input)
        dense = Dense(32, activation='relu')(dense)
        repeat_dense = RepeatVector(predict_interval)(dense)
        main_outputs.append(repeat_dense)

    # concatenate three features
    all_features = keras.layers.concatenate(main_outputs, axis=-1)
    all_dense = TimeDistributed(Dense(32, activation='relu'))(all_features)
    output = TimeDistributed(Dense(3, activation='tanh'))(all_dense)
    model = Model(input=main_inputs, output=output)
    return model


if __name__ == '__main__':
    # >>>>>   AQNet_cnnlstm_lstm_embedding_realvalue
    model = AQNet_cnnlstm_lstm_embedding_realvalue()
    adam = Adam(lr=0.001)
    model.compile(loss='mse', optimizer=adam)
    model.summary()
    hyperparams_name = 'AQNet_cnnlstm_lstm_embedding_realvalue'
    plot_model(model, "{}.png".format(hyperparams_name), show_shapes=True)

    # >>>>>>   AQNet_cnnlstm_lstm_embedding_normalvalue
    model = AQNet_cnnlstm_lstm_embedding_normalvalue()
    adam = Adam(lr=0.001)
    model.compile(loss='mse', optimizer=adam)
    model.summary()
    hyperparams_name = 'AQNet_cnnlstm_lstm_embedding_normalvalue'
    plot_model(model, "{}.png".format(hyperparams_name), show_shapes=True)

    # >>>>>>   AQNet_cnn_sharelstm_embedding_realvalue
    model = AQNet_cnn_sharelstm_embedding_realvalue()
    adam = Adam(lr=0.001)
    model.compile(loss='mse', optimizer=adam)
    model.summary()
    hyperparams_name = 'AQNet_cnn_sharelstm_embedding_realvalue'
    plot_model(model, "{}.png".format(hyperparams_name), show_shapes=True)

    # >>>>>>   AQNet_cnn_sharelstm_embedding_normalvalue
    model = AQNet_cnn_sharelstm_embedding_normalvalue()
    adam = Adam(lr=0.001)
    model.compile(loss='mse', optimizer=adam)
    model.summary()
    hyperparams_name = 'AQNet_cnn_sharelstm_embedding_normalvalue'
    plot_model(model, "{}.png".format(hyperparams_name), show_shapes=True)
