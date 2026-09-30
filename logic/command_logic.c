/* SPDX-License-Identifier: MIT */
/*
 * Copyright (C) 2025 SZ DJI Technology Co., Ltd.
 *  
 * All information contained herein is, and remains, the property of DJI.
 * The intellectual and technical concepts contained herein are proprietary
 * to DJI and may be covered by U.S. and foreign patents, patents in process,
 * and protected by trade secret or copyright law.  Dissemination of this
 * information, including but not limited to data and other proprietary
 * material(s) incorporated within the information, in any form, is strictly
 * prohibited without the express written consent of DJI.
 *
 * If you receive this source code without DJI’s authorization, you may not
 * further disseminate the information, and you must immediately remove the
 * source code and notify DJI of its removal. DJI reserves the right to pursue
 * legal actions against you for any loss(es) or damage(s) caused by your
 * failure to do so.
 */

#include <string.h>
#include <ctype.h>
#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "esp_log.h"
#include "esp_err.h"

#include "ble.h"
#include "data.h"
#include "enums_logic.h"
#include "connect_logic.h"
#include "command_logic.h"
#include "status_logic.h"
#include "dji_protocol_parser.h"
#include "dji_protocol_data_structures.h"

#define TAG "LOGIC_COMMAND"

uint16_t s_current_seq = 0;

uint16_t generate_seq(void) {
    return s_current_seq += 1;
}

/**
 * @brief Send raw bytes directly without protocol frame creation
 *        直接发送原始字节数据，略去协议帧创建环节
 *
 * @param raw_data_string String containing raw bytes in various formats
 *                        包含原始字节的字符串，支持多种格式
 * @param timeout_ms Timeout for waiting result (in milliseconds)
 *                   等待结果的超时时间（以毫秒为单位）
 * 
 * @return esp_err_t ESP_OK on success, error code on failure
 *                   成功返回 ESP_OK，失败返回错误码
 */
esp_err_t command_logic_send_raw_bytes(const char *raw_data_string, int timeout_ms) {
    if (connect_logic_get_state() <= BLE_INIT_COMPLETE) {
        ESP_LOGE(TAG, "BLE not connected");
        return ESP_ERR_INVALID_STATE;
    }

    return data_send_raw_bytes(raw_data_string, timeout_ms);
}

/**
 * @brief General function for constructing data frames and sending commands
 *        构造数据帧并发送命令的通用函数
 *
 * @param cmd_set Command set, used to specify command category
 *                命令集，用于指定命令的类别
 * @param cmd_id Command ID, used to identify specific command
 *               命令 ID，用于标识具体命令
 * @param cmd_type Command type, indicates features like response requirement
 *                 命令类型，指示是否需要应答等特性
 * @param structure Data structure pointer, contains input data for command frame
 *                 数据结构体指针，包含命令帧所需的输入数据
 * @param seq Sequence number, used to match request and response
 *            序列号，用于匹配请求与响应
 * @param timeout_ms Timeout for waiting result (in milliseconds)
 *                   等待结果的超时时间（以毫秒为单位）
 * 
 * Note: The caller needs to free the dynamically allocated memory after using the returned structure.
 * 注意：调用方需要在使用完返回的结构体后释放动态分配的内存。
 * 
 * @return CommandResult Returns parsed structure pointer and data length on success, NULL pointer and length 0 on failure
 *                       成功返回解析后的结构体指针及数据长度，失败返回 NULL 指针及长度 0
 */
CommandResult send_command(uint8_t cmd_set, uint8_t cmd_id, uint8_t cmd_type, const void *input_raw_data, uint16_t seq, int timeout_ms) { 
    CommandResult result = { NULL, 0 };

    if(connect_logic_get_state() <= BLE_INIT_COMPLETE){
        ESP_LOGE(TAG, "BLE not connected");
        return result;
    }

    esp_err_t ret;

    // Create protocol frame
    // 创建协议帧
    size_t frame_length = 0;
    uint8_t *protocol_frame = protocol_create_frame(cmd_set, cmd_id, cmd_type, input_raw_data, seq, &frame_length);
    if (protocol_frame == NULL) {
        ESP_LOGE(TAG, "Failed to create protocol frame");
        return result;
    }

    ESP_LOGI(TAG, "Protocol frame created successfully, length: %zu", frame_length);

    ESP_LOG_BUFFER_HEX_LEVEL(TAG, protocol_frame, frame_length, ESP_LOG_DEBUG);

    void *structure_data = NULL;
    size_t structure_data_length = 0;

    switch (cmd_type) {
        case CMD_NO_RESPONSE:
        case ACK_NO_RESPONSE:
            ret = data_write_without_response(seq, protocol_frame, frame_length);
            if (ret != ESP_OK) {
                ESP_LOGE(TAG, "Failed to send data frame (no response), error: %s", esp_err_to_name(ret));
                free(protocol_frame);
                return result;
            }
            ESP_LOGI(TAG, "Data frame sent without response.");
            break;

        case CMD_RESPONSE_OR_NOT:
        case ACK_RESPONSE_OR_NOT:
            ret = data_write_with_response(seq, protocol_frame, frame_length);
            if (ret != ESP_OK) {
                ESP_LOGE(TAG, "Failed to send data frame (with response), error: %s", esp_err_to_name(ret));
                free(protocol_frame);
                return result;
            }
            ESP_LOGI(TAG, "Data frame sent, waiting for response...");
            
            ret = data_wait_for_result_by_seq(seq, timeout_ms, &structure_data, &structure_data_length);
            if (ret != ESP_OK) {
                ESP_LOGW(TAG, "No result received, but continuing (seq=0x%04X)", seq);
            }

            break;

        case CMD_WAIT_RESULT:
        case ACK_WAIT_RESULT:
            ret = data_write_with_response(seq, protocol_frame, frame_length);
            if (ret != ESP_OK) {
                ESP_LOGE(TAG, "Failed to send data frame (wait result), error: %s", esp_err_to_name(ret));
                free(protocol_frame);
                return result;
            }
            ESP_LOGI(TAG, "Data frame sent, waiting for result...");

            ret = data_wait_for_result_by_seq(seq, timeout_ms, &structure_data, &structure_data_length);
            if (ret != ESP_OK) {
                ESP_LOGE(TAG, "Failed to get parse result for seq=0x%04X, error: 0x%x", seq, ret);
                free(protocol_frame);
                return result;
            }

            if (structure_data == NULL) {
                ESP_LOGE(TAG, "Parse result is NULL for seq=0x%04X", seq);
                free(protocol_frame);
                return result;
            }

            break;

        default:
            ESP_LOGE(TAG, "Invalid cmd_type: %d", cmd_type);
            free(protocol_frame);
            return result;
    }

    free(protocol_frame);
    ESP_LOGI(TAG, "Command executed successfully");

    result.structure = structure_data;
    result.length = structure_data_length;

    return result;
}

/**
 * @brief Switch camera mode
 *        切换相机模式
 *
 * @param mode Camera mode
 *             相机模式
 * 
 * @return camera_mode_switch_response_frame_t* Returns parsed structure pointer, NULL on error
 *                                              返回解析后的结构体指针，如果发生错误返回 NULL
 */
camera_mode_switch_response_frame_t* command_logic_switch_camera_mode(camera_mode_t mode) {
    ESP_LOGI(TAG, "%s: Switching camera mode to: %d", __FUNCTION__, mode);
    if (connect_logic_get_state() != PROTOCOL_CONNECTED) {
        ESP_LOGE(TAG, "Protocol connection to the camera failed. Current connection state: %d", connect_logic_get_state());
        return NULL;
    }

    uint16_t seq = generate_seq();

    camera_mode_switch_command_frame_t command_frame = {
        .device_id = 0xFF330000,
        .mode = mode,
        .reserved = {0x01, 0x47, 0x39, 0x36}  // Reserved field
                                              // 预留字段
    };

    ESP_LOGI(TAG, "Constructed command frame: device_id=0x%08X, mode=%d", (unsigned int)command_frame.device_id, command_frame.mode);

    CommandResult result = send_command(
        0x1D,
        0x04,
        CMD_RESPONSE_OR_NOT,
        &command_frame,
        seq,
        5000
    );

    if (result.structure == NULL) {
        ESP_LOGE(TAG, "Failed to send command or receive response");
        return NULL;
    }

    camera_mode_switch_response_frame_t *response = (camera_mode_switch_response_frame_t *)result.structure;

    ESP_LOGI(TAG, "Received response: ret_code=%d", response->ret_code);
    return response;
}

/**
 * @brief Query device version
 *        查询设备版本号
 *
 * This function sends a query command to get device version information.
 * 该函数通过发送查询命令，获取设备的版本号信息。
 * 
 * The returned version information includes acknowledgment result (`ack_result`), 
 * product ID (`product_id`) and SDK version (`sdk_version`).
 * 返回的版本号信息包括应答结果 (`ack_result`)、产品 ID (`product_id`) 和 SDK 版本号 (`sdk_version`)。
 *
 * @return version_query_response_frame_t* Returns parsed version info structure, NULL on error
 *                                         返回解析后的版本信息结构体，如果发生错误返回 NULL
 */
version_query_response_frame_t* command_logic_get_version(void) {
    ESP_LOGI(TAG, "%s: Querying device version", __FUNCTION__);
    
    if (connect_logic_get_state() != PROTOCOL_CONNECTED) {
        ESP_LOGE(TAG, "Protocol connection to the camera failed. Current connection state: %d", connect_logic_get_state());
        return NULL;
    }

    uint16_t seq = generate_seq();

    CommandResult result = send_command(
        0x00,
        0x00,
        CMD_WAIT_RESULT,
        NULL,
        seq,
        5000
    );

    if (result.structure == NULL) {
        ESP_LOGE(TAG, "Failed to send command or receive response");
        return NULL;
    }

    version_query_response_frame_t *response = (version_query_response_frame_t *)result.structure;

    ESP_LOGI(TAG, "Version Query Response: ack_result=%u, product_id=%s, sdk_version=%.*s",
             response->ack_result, response->product_id, 
             (int)(result.length - (sizeof(uint16_t) + sizeof(response->product_id))),
             response->sdk_version);

    return response;
}

/**
 * @brief Start recording
 *        开始录制
 *
 * @return record_control_response_frame_t* Returns parsed response structure pointer, NULL on error
 *                                          返回解析后的应答结构体指针，如果发生错误返回 NULL
 */
record_control_response_frame_t* command_logic_start_record(void) {
    ESP_LOGI(TAG, "%s: Starting recording", __FUNCTION__);

    if (connect_logic_get_state() != PROTOCOL_CONNECTED) {
        ESP_LOGE(TAG, "Protocol connection to the camera failed. Current connection state: %d", connect_logic_get_state());
        return NULL;
    }

    uint16_t seq = generate_seq();

    record_control_command_frame_t command_frame = {
        .device_id = 0x33FF0000,
        .record_ctrl = 0x00,
        .reserved = {0x00, 0x00, 0x00, 0x00}
    };

    CommandResult result = send_command(
        0x1D,
        0x03,
        CMD_RESPONSE_OR_NOT,
        &command_frame,
        seq,
        5000
    );

    if (result.structure == NULL) {
        ESP_LOGE(TAG, "Failed to send command or receive response");
        return NULL;
    }

    record_control_response_frame_t *response = (record_control_response_frame_t *)result.structure;

    ESP_LOGI(TAG, "Start Record Response: ret_code=%d", response->ret_code);

    return response;
}

/**
 * @brief Stop recording
 *        停止录制
 *
 * @return record_control_response_frame_t* Returns parsed response structure pointer, NULL on error
 *                                          返回解析后的应答结构体指针，如果发生错误返回 NULL
 */
record_control_response_frame_t* command_logic_stop_record(void) {
    ESP_LOGI(TAG, "%s: Stopping recording", __FUNCTION__);

    if (connect_logic_get_state() != PROTOCOL_CONNECTED) {
        ESP_LOGE(TAG, "Protocol connection to the camera failed. Current connection state: %d", connect_logic_get_state());
        return NULL;
    }

    uint16_t seq = generate_seq();

    record_control_command_frame_t command_frame = {
        .device_id = 0x33FF0000,
        .record_ctrl = 0x01,
        .reserved = {0x00, 0x00, 0x00, 0x00}
    };

    CommandResult result = send_command(
        0x1D,
        0x03,
        CMD_RESPONSE_OR_NOT,
        &command_frame,
        seq,
        5000
    );

    if (result.structure == NULL) {
        ESP_LOGE(TAG, "Failed to send command or receive response");
        return NULL;
    }

    record_control_response_frame_t *response = (record_control_response_frame_t *)result.structure;

    ESP_LOGI(TAG, "Stop Record Response: ret_code=%d", response->ret_code);

    return response;
}

/**
 * @brief Push GPS data
 *        推送 GPS 数据
 *
 * @param gps_data Pointer to structure containing GPS data
 *                 指向包含 GPS 数据的结构体
 * 
 * @return gps_data_push_response_frame* Returns parsed response structure pointer, NULL on error
 *                                       返回解析后的应答结构体指针，如果发生错误返回 NULL
 */
gps_data_push_response_frame* command_logic_push_gps_data(const gps_data_push_command_frame *gps_data) {
    ESP_LOGI(TAG, "Pushing GPS data");

    // Check connection status
    // 检查连接状态
    if (connect_logic_get_state() != PROTOCOL_CONNECTED) {
        ESP_LOGE(TAG, "Protocol connection to the camera failed. Current connection state: %d", connect_logic_get_state());
        return NULL;
    }

    if (gps_data == NULL) {
        ESP_LOGE(TAG, "Invalid input: gps_data is NULL");
        return NULL;
    }

    uint16_t seq = generate_seq();

    // Send command and receive response
    // 发送命令并接收应答
    CommandResult result = send_command(
        0x00,
        0x17,
        CMD_NO_RESPONSE,
        gps_data,
        seq,
        5000
    );

    // Return response structure pointer
    // 返回应答结构体指针
    return (gps_data_push_response_frame *)result.structure;
}

/**
 * @brief Quick switch mode key report
 *        快速切换模式按键上报
 *
 * @return key_report_response_frame_t* Returns parsed response structure pointer, NULL on error
 *                                      返回解析后的应答结构体指针，如果发生错误返回 NULL
 */
key_report_response_frame_t* command_logic_key_report_qs(void) {
    ESP_LOGI(TAG, "%s: Reporting key press for mode switch", __FUNCTION__);

    if (connect_logic_get_state() != PROTOCOL_CONNECTED) {
        ESP_LOGE(TAG, "Protocol connection to the camera failed. Current connection state: %d", connect_logic_get_state());
        return NULL;
    }

    uint16_t seq = generate_seq();

    key_report_command_frame_t command_frame = {
        .key_code = 0x02,          // QS key code for mode switch
                                   // QS按键码，模式切换
        .mode = 0x01,              // Fixed as 0x01
                                   // 固定为 0x01
        .key_value = 0x00,         // Fixed as 0x00, short press event
                                   // 固定为 0x00，短按事件
    };

    CommandResult result = send_command(
        0x00,
        0x11,
        CMD_RESPONSE_OR_NOT,
        &command_frame,
        seq,
        5000
    );

    if (result.structure == NULL) {
        ESP_LOGE(TAG, "Failed to send command or receive response");
        return NULL;
    }

    key_report_response_frame_t *response = (key_report_response_frame_t *)result.structure;

    ESP_LOGI(TAG, "Key Report Response: ret_code=%d", response->ret_code);

    return response;
}


key_report_response_frame_t* command_logic_key_report_snapshot(void) {
    ESP_LOGI(TAG, "%s: Reporting key press for snapshot", __FUNCTION__);

    if (connect_logic_get_state() != PROTOCOL_CONNECTED) {
        ESP_LOGE(TAG, "Protocol connection to the camera failed. Current connection state: %d", connect_logic_get_state());
        return NULL;
    }

    uint16_t seq = generate_seq();

    key_report_command_frame_t command_frame = {
        .key_code = 0x03,          // Snapshot key code
                                   // 拍照按键码
        .mode = 0x01,              // Fixed as 0x01
                                   // 固定为 0x01
        .key_value = 0x00,         // Fixed as 0x00, short press event
                                   // 固定为 0x00，短按事件
    };

    CommandResult result = send_command(
        0x00,
        0x11,
        CMD_RESPONSE_OR_NOT,
        &command_frame,
        seq,
        5000
    );
    
    if (result.structure == NULL) {
        ESP_LOGE(TAG, "Failed to send command or receive response");
        return NULL;
    }

    key_report_response_frame_t *response = (key_report_response_frame_t *)result.structure;

    ESP_LOGI(TAG, "Key Report Response: ret_code=%d", response->ret_code);

    return response;
}