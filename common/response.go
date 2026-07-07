package common

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

// Response 统一响应结构
type Response struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data,omitempty"`
}

// PageResponse 分页响应结构
type PageResponse struct {
	Code     int         `json:"code"`
	Message  string      `json:"message"`
	Data     interface{} `json:"data"`
	Total    int64       `json:"total"`
	Page     int         `json:"page"`
	PageSize int         `json:"page_size"`
}

// Success 成功响应
func Success(c *gin.Context, data interface{}) {
	c.JSON(http.StatusOK, Response{
		Code:    200,
		Message: "success",
		Data:    data,
	})
}

// SuccessWithMessage 成功响应（带消息）
func SuccessWithMessage(c *gin.Context, message string, data interface{}) {
	c.JSON(http.StatusOK, Response{
		Code:    200,
		Message: message,
		Data:    data,
	})
}

// Error 错误响应
func Error(c *gin.Context, code int, message string) {
	c.JSON(http.StatusOK, Response{
		Code:    code,
		Message: message,
	})
}

// ErrorWithStatus 错误响应（带HTTP状态码）
func ErrorWithStatus(c *gin.Context, httpStatus int, code int, message string) {
	c.JSON(httpStatus, Response{
		Code:    code,
		Message: message,
	})
}

// PageSuccess 分页成功响应
func PageSuccess(c *gin.Context, data interface{}, total int64, page, pageSize int) {
	c.JSON(http.StatusOK, PageResponse{
		Code:     200,
		Message:  "success",
		Data:     data,
		Total:    total,
		Page:     page,
		PageSize: pageSize,
	})
}

// BadRequest 400 错误
func BadRequest(c *gin.Context, message string) {
	Error(c, 400, message)
}

// Unauthorized 401 错误
func Unauthorized(c *gin.Context, message string) {
	ErrorWithStatus(c, http.StatusUnauthorized, 401, message)
}

// Forbidden 403 错误
func Forbidden(c *gin.Context, message string) {
	ErrorWithStatus(c, http.StatusForbidden, 403, message)
}

// NotFound 404 错误
func NotFound(c *gin.Context, message string) {
	Error(c, 404, message)
}

// InternalServerError 500 错误
func InternalServerError(c *gin.Context, message string) {
	Error(c, 500, message)
}
