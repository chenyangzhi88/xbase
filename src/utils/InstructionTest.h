#pragma once
#include "spdlog/spdlog.h"
#include <immintrin.h>

void m256_load_test() {
    char data[32] = {99, 2, 3, 4, 5, 6, 7, 8, 1, 2, 3, 4, 5, 6, 7, 8, 1, 2, 3, 4, 5, 6, 7, 8, 1, 2, 3, 4, 5, 6, 7, 98}; // 32 字节对齐的整数数组
    __m256i ymm_data = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(data));                                     // 从数组加载到 ymm 寄存器
    char loaded_data[32] = {
        0,
    };
    _mm256_storeu_si256((__m256i *)loaded_data, ymm_data);
    // for (int i = 0; i < 32; i++) {
    //     std::cout << "idx: " << i << ", value: " << (int)loaded_data[i] << std::endl;
    // }
    spdlog::info("loaded data finished");
    return;
}

void MaskEmptyTest() {
    // This only works because ctrl_t::kEmpty is -128.
    int8_t pos[16] = {-128, -128, -128, -128, -128, -128, -128, -128, -128, -128, -128, -128, -128, -128, -128, -128};
    auto match = _mm_set1_epi8(static_cast<char>(int8_t(-128)));
    __m128i ctrl = _mm_loadu_si128(reinterpret_cast<const __m128i *>(pos));
    uint32_t result = static_cast<uint32_t>(_mm_movemask_epi8(_mm_cmpeq_epi8(match, ctrl)));
    spdlog::info("the result is {}", result);
}